import subprocess

# Define el script TMSL
tmsl = '''{
  "refresh": {
    "type": "full",
    "objects": [
      {
        "database": "Bi_Mobo_Reingenieria",
        "table": "Sell_out"
      },
      {
        "database": "Bi_Mobo_Reingenieria",
        "table": "inventario_CEDIS_tienda"
      },
      {
        "database": "Bi_Mobo_Reingenieria",
        "table": "inventario_CEDIS"
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
