& .\scripts\stop_cluster.ps1

Start-Sleep -Milliseconds 500
Remove-Item -Recurse -Force ./data -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path ./data/node1, ./data/node2, ./data/node3, ./data/node4, ./data/node5, ./data/node6, ./data/gateway | Out-Null

Start-Process -FilePath ".\bin\storagenode.exe" -ArgumentList "-addr :8081 -data-dir ./data/node1" -WindowStyle Hidden
Start-Process -FilePath ".\bin\storagenode.exe" -ArgumentList "-addr :8082 -data-dir ./data/node2" -WindowStyle Hidden
Start-Process -FilePath ".\bin\storagenode.exe" -ArgumentList "-addr :8083 -data-dir ./data/node3" -WindowStyle Hidden
Start-Process -FilePath ".\bin\storagenode.exe" -ArgumentList "-addr :8084 -data-dir ./data/node4" -WindowStyle Hidden
Start-Process -FilePath ".\bin\storagenode.exe" -ArgumentList "-addr :8085 -data-dir ./data/node5" -WindowStyle Hidden
Start-Process -FilePath ".\bin\storagenode.exe" -ArgumentList "-addr :8086 -data-dir ./data/node6" -WindowStyle Hidden

Start-Process -FilePath ".\bin\gateway.exe" -ArgumentList "-addr :8080 -metrics-addr :9090 -data-dir ./data/gateway -nodes http://127.0.0.1:8081,http://127.0.0.1:8082,http://127.0.0.1:8083,http://127.0.0.1:8084,http://127.0.0.1:8085,http://127.0.0.1:8086" -WindowStyle Hidden

Start-Sleep -Seconds 3
& .\bin\svctl.exe status
