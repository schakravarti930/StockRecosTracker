# Static Dashboard Migration

The Streamlit app remains in `dashboard/app.py`. The static dashboard has two checkpoints:

1. Generate static JSON from Azure SQL with `scripts/export_dashboard_data.py`.
2. Serve the React dashboard from those static files.

## Local Testing

Generate sample data without Azure SQL:

```powershell
python scripts/export_dashboard_data.py --sample
npm install
npm run dev
```

Generate real data using `.env`:

```powershell
python scripts/export_dashboard_data.py
npm run dev
```

Build the production site:

```powershell
npm run build
```

The generated files under `public/data/` are ignored by Git. They are created locally for testing and in GitHub Actions for deployment.

## dailyohlc Handling

The exporter writes one static file per stock symbol:

```text
public/data/price-history/RELIANCE.NS.json
public/data/price-history/TCS.NS.json
```

Each file contains compact `[date, close]` rows. The React dashboard loads a stock's price file only when that stock is selected in Stock Lookup, so the initial page load does not download all price history.
