SET RUSTFLAGS=-C target-cpu=native
SET RUST_BACKTRACE=1
SET RUST_LOG=warcraider=info
SET REPLICAS=%2
SET OFFSET=0
cargo run --release %1
pause