@echo off
SETLOCAL

rem example call:  runPublisher.bat CHANNAME:sampleChannel SIZE:1 RNAME:nhp://uslxcd001619:9001

set LOCALCP=target\um-pub-sub-example-0.1.0-SNAPSHOT.jar

if exist lib\*.jar (
    for %%D in (lib\*.jar) do call :appendClasspath "%%D"
)

java -classpath "%LOCALCP%" com.amway.integration.um.example.Publisher %*

ENDLOCAL
goto :EOF

:appendClasspath
set LOCALCP=%LOCALCP%;%1
goto :EOF
