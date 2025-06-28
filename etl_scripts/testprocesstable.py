import subprocess

# Define el script TMSL
tmsl = '''{
    "refresh": {
        "type": "full",
        "objects": [
            {
                "database": "Bi_Mobo_Reingenieria",
                "table": "sell_out_cadenas"
            }
        ]
    }
}'''

cmd = [
    "powershell.exe",
    "-ExecutionPolicy", "Bypass",
    "-Command",
    f"Invoke-ASCmd -Server '192.168.10.4\\SQLSERVERDEV' -Query '{tmsl}'"
]

subprocess.run(cmd)
