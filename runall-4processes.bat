SET RUSTFLAGS=-C target-cpu=native
SET PYTHONIOENCODING=UTF-8
SET REPLICAS=4
cargo build --release

REM thanks http://store.tomandmiu.com/cal-amc.php and https://www.mathsisfun.com/binary-decimal-hexadecimal-converter.html

start /affinity F run.bat 1 %REPLICAS%
start /affinity F0 run.bat 2 %REPLICAS%
start /affinity F00 run.bat 3 %REPLICAS%
start /affinity F000 run.bat 4 %REPLICAS%
pause