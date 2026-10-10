# Jito Block Engine gRPC: searcher PoC

A minimal proof of concept of how our searcher authenticates with the Jito
Block Engine over gRPC and submits bundles.

## The project

An on-chain arbitrage searcher on Solana. It follows DEX pools (Pump AMM,
Meteora DLMM / DAMM, Orca Whirlpools, Raydium, Manifest and others) over a
Geyser gRPC stream, prices arbitrage opportunities, and sends each one as a
bundle: one transaction that runs the trade through our on-chain program
(which reverts unless the trade is profitable) and pays the Jito tip. Bundles
go to the nearest Block Engine regions over gRPC. The production bot is
written in Rust (tonic); this file shows the same flow in Python.

## Where

1. **gRPC authentication:** [`BlockEngine.authenticate` and `metadata`](https://github.com/mevichi/jito-searcher-grpc-poc/blob/main/jito_bundle_poc.py#L78-L100).
   Challenge-response with the AuthService: `GenerateAuthChallenge` with the
   auth keypair's public key, sign `<pubkey>-<challenge>`, `GenerateAuthTokens`.
   Every SearcherService call carries `authorization: Bearer <access token>`;
   the access token is renewed with `RefreshAccessToken` before it expires,
   and the handshake is redone when the refresh token expires.
2. **Bundle submission:** [`BlockEngine.send_bundle`](https://github.com/mevichi/jito-searcher-grpc-poc/blob/main/jito_bundle_poc.py#L108-L112) and
   [its use in `main`](https://github.com/mevichi/jito-searcher-grpc-poc/blob/main/jito_bundle_poc.py#L151-L167). A signed versioned
   transaction (the trade, and the tip to an account from `GetTipAccounts`)
   wrapped in a `Bundle` and sent with `SendBundle`;
   `SubscribeBundleResults` reports what became of it.

## Run

```
pip install -r requirements.txt
python jito_bundle_poc.py --auth-keypair auth.json --payer-keypair payer.json --rpc <mainnet RPC URL> [--send]
```

Keypairs are Solana CLI keypair files and stay outside the repo. Without
`--send` it authenticates, reads the tip accounts and builds the bundle
without sending it. Before the auth key is whitelisted, `GenerateAuthTokens`
answers `PERMISSION_DENIED: The supplied pubkey is not authorized to
generate a token.`
