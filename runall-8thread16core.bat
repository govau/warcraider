SET RUSTFLAGS=-C target-cpu=native -C link-args=/STACK:4194304
SET PYTHONIOENCODING=UTF-8
SET REPLICAS=8
cargo build --release

start /affinity 3 run.bat 1 %REPLICAS%
start /affinity C run.bat 2 %REPLICAS%
start /affinity 30 run.bat 3 %REPLICAS%
start /affinity C0 run.bat 4 %REPLICAS%
start /affinity 300 run.bat 5 %REPLICAS%
start /affinity C0 run.bat 6 %REPLICAS%
start /affinity 300 run.bat 7 %REPLICAS%
start /affinity C000 run.bat 8 %REPLICAS%
pause