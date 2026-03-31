Write-Host "Setting up personal-finance-etl..."

python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt

if (-Not (Test-Path .env)) {
    Copy-Item .env.example .env
    Write-Host "Created .env from .env.example — open it and fill in your paths"
} else {
    Write-Host ".env already exists — skipping"
}

Write-Host "Setup complete."
