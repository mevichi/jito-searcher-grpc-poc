#!/usr/bin/env python3
"""Jito Block Engine over gRPC: searcher authentication and bundle submission.

A minimal proof of concept of how our searcher talks to the Block Engine (the
production bot runs the same flow in Rust, with tonic):

1. Authentication (class BlockEngine, authenticate / metadata):
   challenge-response with the AuthService. GenerateAuthChallenge with the
   auth keypair's public key, sign "<pubkey>-<challenge>" with that keypair,
   GenerateAuthTokens. Every SearcherService call then carries
   "authorization: Bearer <access token>"; the access token is renewed with
   RefreshAccessToken before it expires, and the handshake is redone when the
   refresh token expires.

2. Bundle submission (send_bundle, main):
   one versioned transaction (here a 1-lamport self-transfer standing in for
   the trade, plus the Jito tip to one of the accounts GetTipAccounts returns)
   in a Bundle, sent with SendBundle; SubscribeBundleResults reports what
   became of it.

Keys are read from Solana CLI keypair files; this file holds no secrets.

usage:
    pip install -r requirements.txt
    python jito_bundle_poc.py --auth-keypair auth.json --payer-keypair payer.json \\
        --rpc <mainnet RPC URL> [--block-engine frankfurt.mainnet.block-engine.jito.wtf] [--tip 1000] [--send]

Without --send it authenticates, reads the tip accounts and builds the
bundle, but does not send it.
"""
import argparse
import json
import random
import time
import urllib.request

import grpc
from jito_searcher_client.generated.auth_pb2 import (
    GenerateAuthChallengeRequest,
    GenerateAuthTokensRequest,
    RefreshAccessTokenRequest,
    Role,
)
from jito_searcher_client.generated.auth_pb2_grpc import AuthServiceStub
from jito_searcher_client.generated.bundle_pb2 import Bundle
from jito_searcher_client.generated.packet_pb2 import Meta, Packet
from jito_searcher_client.generated.searcher_pb2 import (
    GetTipAccountsRequest,
    SendBundleRequest,
    SubscribeBundleResultsRequest,
)
from jito_searcher_client.generated.searcher_pb2_grpc import SearcherServiceStub
from solders.hash import Hash
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.pubkey import Pubkey
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction

# Renew a token this long before it expires.
TOKEN_MARGIN_S = 60


class BlockEngine:
    """An authenticated gRPC connection to one Block Engine region."""

    def __init__(self, url: str, auth_keypair: Keypair):
        self.channel = grpc.secure_channel(url, grpc.ssl_channel_credentials())
        self.auth = AuthServiceStub(self.channel)
        self.searcher = SearcherServiceStub(self.channel)
        self.keypair = auth_keypair
        self.access = None  # (token, expires at, unix seconds)
        self.refresh = None
        self.authenticate()

    # ---- 1. Authentication -------------------------------------------------

    def authenticate(self):
        """The challenge-response handshake: access and refresh tokens."""
        pubkey = self.keypair.pubkey()
        challenge = self.auth.GenerateAuthChallenge(
            GenerateAuthChallengeRequest(role=Role.SEARCHER, pubkey=bytes(pubkey))
        ).challenge
        message = f"{pubkey}-{challenge}"
        signature = self.keypair.sign_message(message.encode())
        tokens = self.auth.GenerateAuthTokens(
            GenerateAuthTokensRequest(challenge=message, client_pubkey=bytes(pubkey), signed_challenge=bytes(signature))
        )
        self.access = (tokens.access_token.value, tokens.access_token.expires_at_utc.seconds)
        self.refresh = (tokens.refresh_token.value, tokens.refresh_token.expires_at_utc.seconds)

    def metadata(self):
        """The authorization header for a SearcherService call, tokens renewed first if needed."""
        now = time.time()
        if now >= self.refresh[1] - TOKEN_MARGIN_S:
            self.authenticate()
        elif now >= self.access[1] - TOKEN_MARGIN_S:
            renewed = self.auth.RefreshAccessToken(RefreshAccessTokenRequest(refresh_token=self.refresh[0]))
            self.access = (renewed.access_token.value, renewed.access_token.expires_at_utc.seconds)
        return [("authorization", f"Bearer {self.access[0]}")]

    # ---- 2. Bundles ------------------------------------------------------------

    def tip_accounts(self):
        reply = self.searcher.GetTipAccounts(GetTipAccountsRequest(), metadata=self.metadata())
        return [Pubkey.from_string(a) for a in reply.accounts]

    def send_bundle(self, transactions):
        """Sends signed transactions as one bundle; returns its id."""
        packets = [Packet(data=bytes(tx), meta=Meta(size=len(bytes(tx)))) for tx in transactions]
        reply = self.searcher.SendBundle(SendBundleRequest(bundle=Bundle(packets=packets)), metadata=self.metadata())
        return reply.uuid

    def bundle_results(self, timeout_s: float):
        """The stream of results for this searcher's bundles."""
        return self.searcher.SubscribeBundleResults(SubscribeBundleResultsRequest(), metadata=self.metadata(), timeout=timeout_s)


def load_keypair(path: str) -> Keypair:
    """A Solana CLI keypair file (a JSON array of 64 bytes)."""
    with open(path) as f:
        return Keypair.from_bytes(bytes(json.load(f)))


def latest_blockhash(rpc_url: str) -> Hash:
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "getLatestBlockhash", "params": [{"commitment": "confirmed"}]})
    request = urllib.request.Request(rpc_url, body.encode(), {"Content-Type": "application/json"})
    reply = json.load(urllib.request.urlopen(request, timeout=10))
    return Hash.from_string(reply["result"]["value"]["blockhash"])


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--block-engine", default="frankfurt.mainnet.block-engine.jito.wtf")
    parser.add_argument("--auth-keypair", required=True, help="the keypair Jito whitelists for gRPC")
    parser.add_argument("--payer-keypair", required=True, help="the keypair that signs and pays the transaction")
    parser.add_argument("--rpc", required=True, help="a mainnet RPC URL, for a recent blockhash")
    parser.add_argument("--tip", type=int, default=1000, help="Jito tip in lamports (minimum 1000)")
    parser.add_argument("--send", action="store_true", help="send the bundle (otherwise only build it)")
    args = parser.parse_args()

    auth_keypair = load_keypair(args.auth_keypair)
    try:
        engine = BlockEngine(args.block_engine, auth_keypair)
    except grpc.RpcError as e:
        if e.code() == grpc.StatusCode.PERMISSION_DENIED:
            raise SystemExit(f"{auth_keypair.pubkey()} is not whitelisted for gRPC yet: {e.details()}")
        raise
    print(f"authenticated with {args.block_engine}; access token valid until {time.ctime(engine.access[1])}")

    tip_account = random.choice(engine.tip_accounts())
    payer = load_keypair(args.payer_keypair)
    instructions = [
        # Stands in for the trade (the bot's arbitrage instruction).
        transfer(TransferParams(from_pubkey=payer.pubkey(), to_pubkey=payer.pubkey(), lamports=1)),
        # The Jito tip, in the same transaction.
        transfer(TransferParams(from_pubkey=payer.pubkey(), to_pubkey=tip_account, lamports=args.tip)),
    ]
    message = MessageV0.try_compile(payer.pubkey(), instructions, [], latest_blockhash(args.rpc))
    transaction = VersionedTransaction(message, [payer])
    print(f"bundle: 1 transaction {transaction.signatures[0]} ({len(bytes(transaction))} bytes), tip {args.tip} lamports to {tip_account}")
    if not args.send:
        print("not sent (pass --send to send it)")
        return

    bundle_id = engine.send_bundle([transaction])
    print(f"sent: bundle {bundle_id}")
    try:
        for result in engine.bundle_results(timeout_s=30):
            if result.bundle_id == bundle_id:
                print(f"result: {result}")
                break
    except grpc.RpcError as e:
        if e.code() != grpc.StatusCode.DEADLINE_EXCEEDED:
            raise
        print("no result within 30 s")


if __name__ == "__main__":
    main()
