#!/bin/bash
# Skrypt do restartu kontenera Jupyter z nowymi zmiennymi środowiskowymi

echo "🛑 Zatrzymywanie i usuwanie starego kontenera Jupyter..."
docker compose stop jupyter
docker compose rm -f jupyter

echo "🚀 Uruchamianie nowego kontenera Jupyter z poprawionymi zmiennymi środowiskowymi..."
docker compose up -d jupyter

echo "⏳ Oczekiwanie na uruchomienie Jupyter Lab..."
sleep 5

echo "✅ Gotowe! Jupyter Lab powinien być dostępny na http://localhost:8888"
echo ""
echo "Sprawdź logi:"
echo "  docker compose logs -f jupyter"



