import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Twilio REST API: api.twilio.com/2010-04-01/Accounts/{sid}/
// Auth: Basic (AccountSid:AuthToken)
// Pagination: PageSize + NextPageUri in response
// Verified from twilio-go SDK (baseURL: api.twilio.com, client.Api.ListCall, ListMessage)

function twGet(
  ctx: QueryContext,
  path: string,
  params: Record<string, string> = {},
): any {
  const sid = ctx.connection.config.account_sid || "";
  const token = ctx.connection.config.auth_token || "";
  const qs = new URLSearchParams(params).toString();
  const url = `https://api.twilio.com/2010-04-01/Accounts/${sid}${path}.json${qs ? `?${qs}` : ""}`;
  const auth = Buffer.from(`${sid}:${token}`).toString("base64");
  const resp = syncGet(url, { Authorization: `Basic ${auth}` });
  return resp.status === 200 ? resp.body : null;
}

function* twPaginate(ctx: QueryContext, path: string, key: string) {
  let url: string | null = path;
  const sid = ctx.connection.config.account_sid || "";
  const token = ctx.connection.config.auth_token || "";
  const auth = Buffer.from(`${sid}:${token}`).toString("base64");

  while (url) {
    const fullUrl = url.startsWith("http")
      ? url
      : `https://api.twilio.com${url}`;
    const resp = syncGet(fullUrl, { Authorization: `Basic ${auth}` });
    if (resp.status !== 200) return;
    const body = resp.body as any;
    if (!body?.[key]?.length) return;
    yield* body[key];
    url = body.next_page_uri || null;
  }
}

export default function twilio(dl: DriplinePluginAPI) {
  dl.setName("twilio");
  dl.setVersion("0.1.0");
  dl.setConnectionSchema({
    account_sid: {
      type: "string",
      required: true,
      description: "Twilio Account SID",
      env: "TWILIO_ACCOUNT_SID",
    },
    auth_token: {
      type: "string",
      required: true,
      description: "Twilio Auth Token",
      env: "TWILIO_AUTH_TOKEN",
    },
  });

  // /Calls (SDK: client.Api.ListCall)
  dl.registerTable("twilio_calls", {
    description: "Twilio call records",
    columns: [
      { name: "sid", type: "string" },
      { name: "from_number", type: "string" },
      { name: "to_number", type: "string" },
      { name: "status", type: "string" },
      { name: "direction", type: "string" },
      { name: "duration", type: "string" },
      { name: "price", type: "string" },
      { name: "price_unit", type: "string" },
      { name: "start_time", type: "datetime" },
      { name: "end_time", type: "datetime" },
    ],
    *list(ctx) {
      const sid = ctx.connection.config.account_sid || "";
      for (const c of twPaginate(
        ctx,
        `/2010-04-01/Accounts/${sid}/Calls.json?PageSize=1000`,
        "calls",
      )) {
        yield {
          sid: c.sid || "",
          from_number: c.from || c.from_formatted || "",
          to_number: c.to || c.to_formatted || "",
          status: c.status || "",
          direction: c.direction || "",
          duration: c.duration || "",
          price: c.price || "",
          price_unit: c.price_unit || "",
          start_time: c.start_time || "",
          end_time: c.end_time || "",
        };
      }
    },
  });

  // /Messages (SDK: client.Api.ListMessage)
  dl.registerTable("twilio_messages", {
    description: "Twilio SMS/MMS messages",
    columns: [
      { name: "sid", type: "string" },
      { name: "from_number", type: "string" },
      { name: "to_number", type: "string" },
      { name: "body", type: "string" },
      { name: "status", type: "string" },
      { name: "direction", type: "string" },
      { name: "price", type: "string" },
      { name: "price_unit", type: "string" },
      { name: "date_sent", type: "datetime" },
      { name: "date_created", type: "datetime" },
    ],
    *list(ctx) {
      const sid = ctx.connection.config.account_sid || "";
      for (const m of twPaginate(
        ctx,
        `/2010-04-01/Accounts/${sid}/Messages.json?PageSize=1000`,
        "messages",
      )) {
        yield {
          sid: m.sid || "",
          from_number: m.from || "",
          to_number: m.to || "",
          body: (m.body || "").slice(0, 500),
          status: m.status || "",
          direction: m.direction || "",
          price: m.price || "",
          price_unit: m.price_unit || "",
          date_sent: m.date_sent || "",
          date_created: m.date_created || "",
        };
      }
    },
  });

  // /IncomingPhoneNumbers
  dl.registerTable("twilio_phone_numbers", {
    description: "Twilio phone numbers",
    columns: [
      { name: "sid", type: "string" },
      { name: "phone_number", type: "string" },
      { name: "friendly_name", type: "string" },
      { name: "status", type: "string" },
      { name: "capabilities", type: "json" },
      { name: "date_created", type: "datetime" },
    ],
    *list(ctx) {
      const sid = ctx.connection.config.account_sid || "";
      for (const p of twPaginate(
        ctx,
        `/2010-04-01/Accounts/${sid}/IncomingPhoneNumbers.json?PageSize=1000`,
        "incoming_phone_numbers",
      )) {
        yield {
          sid: p.sid || "",
          phone_number: p.phone_number || "",
          friendly_name: p.friendly_name || "",
          status: p.status || "",
          capabilities: JSON.stringify(p.capabilities || {}),
          date_created: p.date_created || "",
        };
      }
    },
  });

  // /Accounts (the current account)
  dl.registerTable("twilio_account", {
    description: "Twilio account info",
    columns: [
      { name: "sid", type: "string" },
      { name: "friendly_name", type: "string" },
      { name: "status", type: "string" },
      { name: "type", type: "string" },
      { name: "date_created", type: "datetime" },
    ],
    *list(ctx) {
      const body = twGet(ctx, "");
      if (!body) return;
      yield {
        sid: body.sid || "",
        friendly_name: body.friendly_name || "",
        status: body.status || "",
        type: body.type || "",
        date_created: body.date_created || "",
      };
    },
  });
}
