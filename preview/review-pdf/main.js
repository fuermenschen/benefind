import nunjucks from "nunjucks/browser/nunjucks.js";
import templateSource from "/config/review_pdf_template.html?raw";

const env = new nunjucks.Environment();

function formatDateTimeDisplay(date) {
  const pad2 = (value) => String(value).padStart(2, "0");
  return `${pad2(date.getDate())}.${pad2(date.getMonth() + 1)}.${date.getFullYear()} ${pad2(date.getHours())}:${pad2(date.getMinutes())}`;
}

env.addFilter("chf_thousands", (value) => {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  const amount = Number.parseFloat(String(value));
  if (!Number.isFinite(amount)) {
    return "-";
  }
  return Math.trunc(amount).toLocaleString("en-US").replace(/,/g, "'");
});

const fallbackContext = {
  org: {
    id: "org_preview",
    name: "Preview Organization",
    location: "Winterthur",
  },
  map: {
    status: "ok",
    data_uri: null,
    has_image: false,
  },
  classify_payloads: {
    q04: {},
    q05: {},
    q06: {},
  },
  meta: {
    generated_at: new Date().toISOString(),
    generated_at_display: formatDateTimeDisplay(new Date()),
    org_number: 1,
    total_orgs: 1,
  },
};

async function loadContext() {
  try {
    const response = await fetch("/preview/review-pdf/context.real.json", {
      cache: "no-store",
    });
    if (!response.ok) {
      return fallbackContext;
    }
    const context = await response.json();
    const generatedAtRaw = context?.meta?.generated_at;
    const parsed = generatedAtRaw ? new Date(generatedAtRaw) : new Date();
    const validDate = Number.isNaN(parsed.getTime()) ? new Date() : parsed;
    context.meta = {
      ...context.meta,
      generated_at_display: formatDateTimeDisplay(validDate),
      org_number: context?.meta?.org_number ?? 1,
      total_orgs: context?.meta?.total_orgs ?? 1,
    };
    return context;
  } catch {
    return fallbackContext;
  }
}

async function render() {
  const context = await loadContext();
  const frame = document.getElementById("preview-frame");
  try {
    const rendered = env.renderString(templateSource, context);
    frame.srcdoc = rendered;
  } catch (error) {
    const message =
      error instanceof Error ? error.stack || error.message : String(error);
    frame.srcdoc = `<!doctype html><html><body style="font-family: ui-monospace, SFMono-Regular, Menlo, monospace; padding: 16px; color: #991b1b;"><h2>Template render error</h2><pre>${message.replace(/</g, "&lt;")}</pre></body></html>`;
  }
}

render();
