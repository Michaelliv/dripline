import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

const API = "https://api.stripe.com/v1";

function stripeGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const key = ctx.connection.config.api_key || "";
  const qs = new URLSearchParams(params).toString();
  const url = `${API}${path}${qs ? `?${qs}` : ""}`;
  const auth = Buffer.from(`${key}:`).toString("base64");
  const resp = syncGet(url, { Authorization: `Basic ${auth}` });
  return resp.status === 200 ? resp.body : null;
}

function* stripePaginate(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
) {
  let startingAfter = "";
  while (true) {
    const p: Record<string, string> = { ...params, limit: "100" };
    if (startingAfter) p.starting_after = startingAfter;
    const body = stripeGet(ctx, path, p);
    if (!body?.data?.length) return;
    yield* body.data;
    if (!body.has_more) return;
    startingAfter = body.data[body.data.length - 1].id;
  }
}

function ts(epoch: number | null): string {
  return epoch ? new Date(epoch * 1000).toISOString() : "";
}

function cents(amount: number | null): number {
  return amount ? amount / 100 : 0;
}

export default function stripe(dl: DriplinePluginAPI) {
  dl.setName("stripe");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    api_key: {
      type: "string",
      required: true,
      description: "Stripe secret API key (sk_...)",
      env: "STRIPE_API_KEY",
    },
  });

  dl.registerTable("stripe_customers", {
    description: "Stripe customers",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "email", type: "string" },
      { name: "description", type: "string" },
      { name: "currency", type: "string" },
      { name: "balance", type: "number" },
      { name: "delinquent", type: "boolean" },
      { name: "created", type: "datetime" },
      { name: "metadata", type: "json" },
    ],
    *list(ctx) {
      for (const c of stripePaginate(ctx, "/customers")) {
        yield {
          id: c.id,
          name: c.name || "",
          email: c.email || "",
          description: c.description || "",
          currency: c.currency || "",
          balance: cents(c.balance),
          delinquent: c.delinquent ? 1 : 0,
          created: ts(c.created),
          metadata: JSON.stringify(c.metadata || {}),
        };
      }
    },
  });

  dl.registerTable("stripe_subscriptions", {
    description: "Stripe subscriptions",
    columns: [
      { name: "id", type: "string" },
      { name: "customer", type: "string" },
      { name: "status", type: "string" },
      { name: "currency", type: "string" },
      { name: "current_period_start", type: "datetime" },
      { name: "current_period_end", type: "datetime" },
      { name: "cancel_at_period_end", type: "boolean" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      for (const s of stripePaginate(ctx, "/subscriptions")) {
        yield {
          id: s.id,
          customer: s.customer || "",
          status: s.status || "",
          currency: s.currency || "",
          current_period_start: ts(s.current_period_start),
          current_period_end: ts(s.current_period_end),
          cancel_at_period_end: s.cancel_at_period_end ? 1 : 0,
          created: ts(s.created),
        };
      }
    },
  });

  dl.registerTable("stripe_invoices", {
    description: "Stripe invoices",
    columns: [
      { name: "id", type: "string" },
      { name: "customer", type: "string" },
      { name: "customer_email", type: "string" },
      { name: "status", type: "string" },
      { name: "currency", type: "string" },
      { name: "amount_due", type: "number" },
      { name: "amount_paid", type: "number" },
      { name: "total", type: "number" },
      { name: "created", type: "datetime" },
      { name: "due_date", type: "datetime" },
      { name: "hosted_invoice_url", type: "string" },
    ],
    *list(ctx) {
      for (const i of stripePaginate(ctx, "/invoices")) {
        yield {
          id: i.id,
          customer: i.customer || "",
          customer_email: i.customer_email || "",
          status: i.status || "",
          currency: i.currency || "",
          amount_due: cents(i.amount_due),
          amount_paid: cents(i.amount_paid),
          total: cents(i.total),
          created: ts(i.created),
          due_date: ts(i.due_date),
          hosted_invoice_url: i.hosted_invoice_url || "",
        };
      }
    },
  });

  dl.registerTable("stripe_charges", {
    description: "Stripe charges",
    columns: [
      { name: "id", type: "string" },
      { name: "customer", type: "string" },
      { name: "amount", type: "number" },
      { name: "currency", type: "string" },
      { name: "status", type: "string" },
      { name: "paid", type: "boolean" },
      { name: "refunded", type: "boolean" },
      { name: "description", type: "string" },
      { name: "receipt_url", type: "string" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      for (const c of stripePaginate(ctx, "/charges")) {
        yield {
          id: c.id,
          customer: c.customer || "",
          amount: cents(c.amount),
          currency: c.currency || "",
          status: c.status || "",
          paid: c.paid ? 1 : 0,
          refunded: c.refunded ? 1 : 0,
          description: c.description || "",
          receipt_url: c.receipt_url || "",
          created: ts(c.created),
        };
      }
    },
  });

  dl.registerTable("stripe_products", {
    description: "Stripe products",
    columns: [
      { name: "id", type: "string" },
      { name: "name", type: "string" },
      { name: "description", type: "string" },
      { name: "active", type: "boolean" },
      { name: "default_price", type: "string" },
      { name: "created", type: "datetime" },
      { name: "metadata", type: "json" },
    ],
    *list(ctx) {
      for (const p of stripePaginate(ctx, "/products")) {
        yield {
          id: p.id,
          name: p.name || "",
          description: p.description || "",
          active: p.active ? 1 : 0,
          default_price: p.default_price || "",
          created: ts(p.created),
          metadata: JSON.stringify(p.metadata || {}),
        };
      }
    },
  });

  dl.registerTable("stripe_prices", {
    description: "Stripe prices",
    columns: [
      { name: "id", type: "string" },
      { name: "product", type: "string" },
      { name: "active", type: "boolean" },
      { name: "currency", type: "string" },
      { name: "unit_amount", type: "number" },
      { name: "recurring_interval", type: "string" },
      { name: "type", type: "string" },
      { name: "created", type: "datetime" },
    ],
    *list(ctx) {
      for (const p of stripePaginate(ctx, "/prices")) {
        yield {
          id: p.id,
          product: p.product || "",
          active: p.active ? 1 : 0,
          currency: p.currency || "",
          unit_amount: cents(p.unit_amount),
          type: p.type || "",
          recurring_interval: p.recurring?.interval || "",
          created: ts(p.created),
        };
      }
    },
  });
}
