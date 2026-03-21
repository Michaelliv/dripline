import type { DriplinePluginAPI, QueryContext } from "dripline";
import { syncGet } from "dripline";

// Yahoo Finance API (used by piquette/finance-go under the hood)
const YF_API = "https://query2.finance.yahoo.com";

function getQual(ctx: QueryContext, n: string) {
  return ctx.quals.find((q) => q.column === n)?.value;
}

export default function finance(dl: DriplinePluginAPI) {
  dl.setName("finance");
  dl.setVersion("0.1.0");

  // Yahoo Finance quote API (same as steampipe's finance_quote using piquette/finance-go)
  dl.registerTable("finance_quote", {
    description: "Stock/crypto/ETF quote from Yahoo Finance",
    columns: [
      { name: "symbol", type: "string" },
      { name: "short_name", type: "string" },
      { name: "long_name", type: "string" },
      { name: "regular_market_price", type: "number" },
      { name: "regular_market_change", type: "number" },
      { name: "regular_market_change_percent", type: "number" },
      { name: "regular_market_volume", type: "number" },
      { name: "regular_market_open", type: "number" },
      { name: "regular_market_day_high", type: "number" },
      { name: "regular_market_day_low", type: "number" },
      { name: "regular_market_previous_close", type: "number" },
      { name: "fifty_two_week_high", type: "number" },
      { name: "fifty_two_week_low", type: "number" },
      { name: "fifty_day_average", type: "number" },
      { name: "two_hundred_day_average", type: "number" },
      { name: "market_cap", type: "number" },
      { name: "currency", type: "string" },
      { name: "exchange", type: "string" },
      { name: "quote_type", type: "string" },
      { name: "market_state", type: "string" },
    ],
    keyColumns: [{ name: "symbol", required: "required", operators: ["="] }],
    *list(ctx) {
      const symbol = getQual(ctx, "symbol");
      if (!symbol) return;

      // v7 quote endpoint is blocked, use v8 chart API meta for quote data
      const symbols = symbol.split(",").map((s: string) => s.trim());
      for (const sym of symbols) {
        const resp = syncGet(
          `${YF_API}/v8/finance/chart/${encodeURIComponent(sym)}?interval=1d&range=5d`,
          { "User-Agent": "Mozilla/5.0" },
        );
        if (resp.status !== 200) continue;
        const body = resp.body as any;
        const meta = body?.chart?.result?.[0]?.meta;
        if (!meta) continue;

        yield {
          symbol: meta.symbol || sym,
          short_name: meta.shortName || "",
          long_name: meta.longName || "",
          regular_market_price: meta.regularMarketPrice || 0,
          regular_market_change: 0,
          regular_market_change_percent: 0,
          regular_market_volume: meta.regularMarketVolume || 0,
          regular_market_open: 0,
          regular_market_day_high: meta.regularMarketDayHigh || 0,
          regular_market_day_low: meta.regularMarketDayLow || 0,
          regular_market_previous_close:
            meta.chartPreviousClose || meta.previousClose || 0,
          fifty_two_week_high: meta.fiftyTwoWeekHigh || 0,
          fifty_two_week_low: meta.fiftyTwoWeekLow || 0,
          fifty_day_average: 0,
          two_hundred_day_average: 0,
          market_cap: 0,
          currency: meta.currency || "",
          exchange: meta.exchangeName || meta.fullExchangeName || "",
          quote_type: meta.instrumentType || "",
          market_state: "",
        };
      }
    },
  });

  // Yahoo Finance chart API for historical data
  dl.registerTable("finance_quote_daily", {
    description: "Daily historical price data from Yahoo Finance",
    columns: [
      { name: "symbol", type: "string" },
      { name: "timestamp", type: "datetime" },
      { name: "open", type: "number" },
      { name: "high", type: "number" },
      { name: "low", type: "number" },
      { name: "close", type: "number" },
      { name: "volume", type: "number" },
    ],
    keyColumns: [
      { name: "symbol", required: "required", operators: ["="] },
      { name: "range", required: "optional", operators: ["="] },
    ],
    *list(ctx) {
      const symbol = getQual(ctx, "symbol");
      if (!symbol) return;
      const range = getQual(ctx, "range") || "1mo"; // 1d, 5d, 1mo, 3mo, 6mo, 1y, 5y, max

      const resp = syncGet(
        `${YF_API}/v8/finance/chart/${encodeURIComponent(symbol)}?interval=1d&range=${range}`,
        { "User-Agent": "Mozilla/5.0" },
      );
      if (resp.status !== 200) return;
      const body = resp.body as any;
      const result = body?.chart?.result?.[0];
      if (!result) return;

      const timestamps = result.timestamp || [];
      const quote = result.indicators?.quote?.[0] || {};

      for (let i = 0; i < timestamps.length; i++) {
        yield {
          symbol,
          timestamp: new Date(timestamps[i] * 1000).toISOString(),
          open: quote.open?.[i] || 0,
          high: quote.high?.[i] || 0,
          low: quote.low?.[i] || 0,
          close: quote.close?.[i] || 0,
          volume: quote.volume?.[i] || 0,
        };
      }
    },
  });
}
