$apiKey = "15d3b1c6-ece7-4483-913d-e16c73eb7320" # SUBSTITUA pela sua chave real (ex: 15d3b1c6-ece7-4483-913d-e16c73eb7320)
$schemaPath = "pdfs_para_teste/dataset.json" # Caminho para o seu schema
$pdfFolder = "pdfs_para_teste" # Caminho para a pasta dos PDFs

# --- Mapeia os arquivos e constrói o formulário ---

$schemaFile = Get-Item $schemaPath
$pdfFiles = @(
    (Get-Item "$pdfFolder/oab_1.pdf"),
    (Get-Item "$pdfFolder/oab_2.pdf"),
    (Get-Item "$pdfFolder/oab_3.pdf"),
    (Get-Item "$pdfFolder/tela_sistema_1.pdf"),
    (Get-Item "$pdfFolder/tela_sistema_2.pdf"),
    (Get-Item "$pdfFolder/tela_sistema_3.pdf")
)

# Cria o Hash Table para o corpo (FormData)
$formBody = @{ 
    extraction_schema = $schemaFile 
}

# Adiciona cada PDF ao corpo sob a chave 'pdf_files'
foreach ($file in $pdfFiles) {
    # Invoke-RestMethod permite múltiplos arquivos com a mesma chave 'pdf_files'
    $formBody["pdf_files"] += $file
}

# --- Envia a requisição ---
Invoke-RestMethod -Uri "http://127.0.0.1:8000/extract/" `
    -Method Post `
    -Headers @{ "X-API-Key" = $apiKey } `
    -Form $formBody `
    -ContentType "multipart/form-data"