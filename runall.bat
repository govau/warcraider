cargo build --release
start /affinity 3 run.bat 1
start /affinity C run.bat 2
start /affinity 30 run.bat 3
start /affinity C0 run.bat 4
start /affinity 300 run.bat 5
start /affinity C0 run.bat 6
start /affinity 300 run.bat 7
start /affinity C000 run.bat 8

REM start /affinity F run.bat 2
REM start /affinity F0 run.bat 3
REM start /affinity F00 run.bat 6
REM start /affinity F000 run.bat 7
pause