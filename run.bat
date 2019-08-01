SET RUSTFLAGS=-C target-cpu=native -C link-args=/STACK:4194304
SET RUST_BACKTRACE=1
SET RUST_LOG=warcraider=info
SET REPLICAS=%2
SET OFFSET=37
cargo run --release %1
pause