import pdfplumber
import requests
import pyodbc
import time
import io
import os   
from openai import OpenAI
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ──────────────────────────────────────────────────────────────────

USE_GEMINI = True  # Switch to True to use Gemini 2.5 Flash

GEMINI_API_KEY = "AIzaSyByFYUHBd16puz0ypNcbs-9ZwZAL4-1ASI"
LM_STUDIO_URL  = "http://localhost:1234/v1"
LM_STUDIO_MODEL = "nemotron-3-nano-4b"  # match exact name in LM Studio

DB_CONN_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={os.getenv('AZURE_SQL_SERVER')}.database.windows.net;"
    f"DATABASE={os.getenv('AZURE_SQL_DATABASE')};"
    f"UID={os.getenv('AZURE_SQL_USERNAME')};"
    f"PWD={os.getenv('AZURE_SQL_PASSWORD')};"
    f"Encrypt=yes;"
    f"TrustServerCertificate=no;"
    f"Connection Timeout=30;"
)

# ── PROMPT ───────────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a financial analyst summarizing sell-side equity research reports.

Your output will be shown on a dashboard. Users read the LABEL first — it must tell them 
WHY the analyst gave this rating in plain english, in under 10 words.
The PARAGRAPH gives the full reasoning behind that label.

Respond in exactly this format with no extra text:

THESIS_LABEL: <under 10 words — the single core reason for the rating>
THESIS: <2-3 sentences — the analyst's main argument for the buy/sell/hold rating>

VALUATION_LABEL: <under 10 words — key valuation concern or comfort>
VALUATION: <2-3 sentences — price target, methodology, upside/downside and what drives it>

RISKS_LABEL: <under 10 words — the biggest specific risk to the thesis>
RISKS: <2-3 sentences — what could go wrong and by how much>

CATALYSTS_LABEL: <under 10 words — what could change the outlook positively>
CATALYSTS: <2-3 sentences — specific triggers or events to watch>

Rules for labels:
- Must explain the WHY, not just state the what
- No jargon or ticker names
- No numbers unless they are the point
- Should make sense to someone who hasn't read the report
- If a section is not covered in the report write:
  LABEL: Insufficient information in report
  PARAGRAPH: This report does not cover this aspect explicitly.

---

EXAMPLE INPUT:
InterGlobe Aviation (INDIGO IN) | Rating: HOLD | CMP: Rs4,861 | TP: Rs5,236
New FDTL crew-rostering norms triggered cancellation of ~4,500 flights and a DGCA-mandated 
10% capacity cut. Pilot shortage (only 1,213 licenses issued in 2024) limits fresh hiring. 
Employee cost per ASKM rising from Rs0.47 to Rs0.62 by FY28E. EBITDAR cut 13%/8%/12% 
for FY26/27/28E. Damp lease aircraft can provide near-term relief. Yield expected to 
recover to Rs5.30 by FY28E.

EXAMPLE OUTPUT:
THESIS_LABEL: Pilot shortage makes growth legally impossible now
THESIS: A new regulation capping flight duty hours has exposed a structural pilot shortage at IndiGo, forcing DGCA to mandate a 10% capacity cut and triggering 4,500 flight cancellations. With only 1,213 new pilot licenses issued nationally in 2024, the airline cannot hire fast enough to meet the new norms, capping capacity growth at 8% CAGR versus earlier high-teens expectations. The analyst downgrades to HOLD as this is a structural problem, not a one-quarter blip.

VALUATION_LABEL: Fair value already priced in at current levels
VALUATION: The stock is valued at 10.5x FY27E EBITDAR, arriving at a revised target price of Rs 5,236 — implying only 7.7% upside from current levels of Rs 4,861. EBITDAR estimates have been cut 8-13% across FY26-28E and EPS slashed by 20-27%, reflecting higher employee costs and lower capacity. The narrow upside is what keeps this a HOLD rather than a Reduce.

RISKS_LABEL: Cost inflation worsens if hiring market stays tight
RISKS: Employee cost per available seat kilometre is expected to rise 32% from Rs 0.47 in FY25 to Rs 0.62 by FY28E, assuming 11% annual salary growth and 670 new pilot inductions per year. If the pilot supply remains constrained, IndiGo may need to pay above-market salaries to attract talent, pushing costs even higher and compressing the RASK-CASK spread which is already expected to hit near-zero in FY26E. A prolonged shortage could push the rating to Reduce.

CATALYSTS_LABEL: Borrowed aircraft can bridge the pilot gap near term
CATALYSTS: IndiGo currently has only 8 damp lease aircraft (where the lessor provides the pilot), well below its historical peak of 33, giving meaningful headroom to add capacity without needing to hire pilots immediately. Declining aircraft and engine rentals from Rs 30bn in FY25 to Rs 13.4bn by FY28E will provide a steady margin tailwind. A faster-than-expected resolution of the pilot supply issue or DGCA relaxing FDTL norms would be the key re-rating trigger.

---

Now summarize the following report in the same format:"""

# ── PDF ───────────────────────────────────────────────────────────────────────

def download_pdf(url):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/pdf,*/*",
        "Referer": url.split("/")[0] + "//" + url.split("/")[2] + "/"
    }
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 403:
        raise Exception(f"Access blocked (403): {url}")
    elif response.status_code == 404:
        raise Exception(f"Not found (404): {url}")
    response.raise_for_status()
    return io.BytesIO(response.content)
 
def extract_text(pdf_bytes):
    full_text = ""
    with pdfplumber.open(pdf_bytes) as pdf:
        for page in pdf.pages:  
            text = page.extract_text() or ""
            full_text += text + "\n"
            for table in page.extract_tables():
                for row in table:
                    cleaned = [cell or "" for cell in row]
                    full_text += " | ".join(cleaned) + "\n"
                full_text += "\n"
    return full_text.strip()

# ── LLM ───────────────────────────────────────────────────────────────────────

def summarize_lmstudio(text):
    client = OpenAI(base_url=LM_STUDIO_URL, api_key="lm-studio")
    response = client.chat.completions.create(
        model=LM_STUDIO_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": f"Summarize this report:\n\n{text[:8000]}"}
        ],
        temperature=0.2,
        max_tokens=700
    )
    return response.choices[0].message.content

def summarize_gemini(text):
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel(
        model_name="gemini-2.5-flash",
        system_instruction=SYSTEM_PROMPT
    )
    response = model.generate_content(
        f"Summarize this report:\n\n{text[:8000]}",
        generation_config=genai.GenerationConfig(
            temperature=0.2,
            max_output_tokens=700
        )
    )
    return response.text

def summarize(text):
    if USE_GEMINI:
        return summarize_gemini(text)
    return summarize_lmstudio(text)

# ── PARSER ────────────────────────────────────────────────────────────────────

def parse_summary(response_text):
    fields = {
        "thesis_label":    "",
        "thesis":          "",
        "valuation_label": "",
        "valuation":       "",
        "risks_label":     "",
        "risks":           "",
        "catalysts_label": "",
        "catalysts":       ""
    }
    mappings = {
        "THESIS_LABEL:":    "thesis_label",
        "THESIS:":          "thesis",
        "VALUATION_LABEL:": "valuation_label",
        "VALUATION:":       "valuation",
        "RISKS_LABEL:":     "risks_label",
        "RISKS:":           "risks",
        "CATALYSTS_LABEL:": "catalysts_label",
        "CATALYSTS:":       "catalysts"
    }

    current_key = None
    for line in response_text.splitlines():
        line = line.strip()
        matched = False
        for marker, key in mappings.items():
            if line.startswith(marker):
                current_key = key
                fields[current_key] = line[len(marker):].strip()
                matched = True
                break
        if not matched and current_key and line:
            fields[current_key] += " " + line

    # Clean up extra whitespace
    return {k: v.strip() for k, v in fields.items()}

# ── DATABASE ──────────────────────────────────────────────────────────────────

def get_pending_reports(cursor):
    cursor.execute("""
        SELECT top 10 r.id, r.attachment_url, r.stock_code
        FROM dbo.recommendations r
        LEFT JOIN report_summaries s ON r.id = s.report_id
        WHERE s.id IS NULL
    """)
    return cursor.fetchall()

def save_summary(cursor, conn, report_id, pdf_url, ticker, fields, status="done"):
    cursor.execute("""
        INSERT INTO report_summaries (
            report_id, pdf_url, ticker,
            thesis_label, thesis,
            valuation_label, valuation,
            risks_label, risks,
            catalysts_label, catalysts,
            status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        report_id, pdf_url, ticker,
        fields.get("thesis_label",    ""),
        fields.get("thesis",          ""),
        fields.get("valuation_label", ""),
        fields.get("valuation",       ""),
        fields.get("risks_label",     ""),
        fields.get("risks",           ""),
        fields.get("catalysts_label", ""),
        fields.get("catalysts",       ""),
        status
    )
    conn.commit()

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    conn   = pyodbc.connect(DB_CONN_STRING)
    cursor = conn.cursor()

    reports   = get_pending_reports(cursor)
    total     = len(reports)
    print(f"Found {total} unprocessed reports")
    print(f"Using: {'Gemini 2.5 Flash' if USE_GEMINI else 'LM Studio / Nemotron'}\n")

    for i, (report_id, attachment_url, stock_code) in enumerate(reports):
        print(f"[{i+1}/{total}] {stock_code} — {attachment_url[:60]}...")

        try:
            pdf_bytes = download_pdf(attachment_url)
            text      = extract_text(pdf_bytes)

            if not text.strip():
                print("  EMPTY — skipping")
                save_summary(cursor, conn, report_id, attachment_url, stock_code, {}, status="empty")
                continue

            raw      = summarize(text)
            fields   = parse_summary(raw)
            save_summary(cursor, conn, report_id, attachment_url, stock_code, fields, status="done")
            print(f"  ✓ {fields['thesis_label']}")

        except Exception as e:
            print(f"  ERROR: {e}")
            save_summary(cursor, conn, report_id, attachment_url, stock_code, {}, status=str(e)[:200])

        # Rate limiting
        if USE_GEMINI:
            time.sleep(6)   # stay under 10 RPM free tier
        else:
            time.sleep(2)   # give GPU a breather

    print("\nDone.")
    conn.close()

if __name__ == "__main__":
    main()