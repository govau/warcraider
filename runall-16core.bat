SET RUSTFLAGS=-C target-cpu=native -C link-args=/STACK:4194304
SET PYTHONIOENCODING=UTF-8
SET REPLICAS=16
cargo build --release

REM thanks http://store.tomandmiu.com/cal-amc.php and https://www.mathsisfun.com/binary-decimal-hexadecimal-converter.html

start /affinity 1 run.bat 1 %REPLICAS%
start /affinity 2 run.bat 2 %REPLICAS%
start /affinity 4 run.bat 3 %REPLICAS%
start /affinity 8 run.bat 4 %REPLICAS%
start /affinity 10 run.bat 5 %REPLICAS%
start /affinity 20 run.bat 6 %REPLICAS%
start /affinity 40 run.bat 7 %REPLICAS%
start /affinity 80 run.bat 8 %REPLICAS%
start /affinity 100 run.bat 9 %REPLICAS%
start /affinity 200 run.bat 10 %REPLICAS%
start /affinity 400 run.bat 11 %REPLICAS%
start /affinity 800 run.bat 12 %REPLICAS%
start /affinity 1000 run.bat 13 %REPLICAS%
start /affinity 2000 run.bat 14 %REPLICAS%
start /affinity 4000 run.bat 15 %REPLICAS%
start /affinity 8000 run.bat 16 %REPLICAS%

pause