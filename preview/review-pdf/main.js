import nunjucks from "nunjucks/browser/nunjucks.js";
import templateSource from "/config/review_pdf_template.html?raw";

const fallbackContext = {
  org: {
    id: "org_preview",
    name: "Preview Organization",
    location: "Winterthur"
  },
  map: {
    status: "ok",
    data_uri: null,
    has_image: false
  },
  classify_payloads: {
    q04_primary_target_group: {},
    q05_founded_year: {},
    q06_financials_manual: {}
  },
  meta: {
    generated_at: new Date().toISOString()
  }
};

async function loadContext() {
  try {
    const response = await fetch("/preview/review-pdf/context.real.json", { cache: "no-store" });
    if (!response.ok) {
      return fallbackContext;
    }
    return await response.json();
  } catch {
    return fallbackContext;
  }
}

async function render() {
  const context = await loadContext();
  const frame = document.getElementById("preview-frame");
  try {
    const rendered = nunjucks.renderString(templateSource, context);
    frame.srcdoc = rendered;
  } catch (error) {
    const message = error instanceof Error ? error.stack || error.message : String(error);
    frame.srcdoc = `<!doctype html><html><body style="font-family: ui-monospace, SFMono-Regular, Menlo, monospace; padding: 16px; color: #991b1b;"><h2>Template render error</h2><pre>${message.replace(/</g, "&lt;")}</pre></body></html>`;
  }
}

render();
