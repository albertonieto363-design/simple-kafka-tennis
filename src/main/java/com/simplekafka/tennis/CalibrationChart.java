package com.simplekafka.tennis;

import java.util.List;
import java.util.Locale;

/**
 * Renders calibration reports as one self-contained HTML page with a reliability chart per
 * snapshot time. No libraries and no network needed: open the file in any browser.
 *
 * Reading a panel: each dot is a price bucket. Across is the average price of the favorite
 * in that bucket, up is how often the favorite really won. A dot on the gray line means the
 * market was right on average. The vertical bar is the 95% range, which is wide when a bucket
 * holds few matches.
 */
public final class CalibrationChart {
    private CalibrationChart() {
    }

    // Plot geometry in SVG units.
    private static final int LEFT = 52;
    private static final int TOP = 14;
    private static final int RIGHT = 14;
    private static final int BOTTOM = 48;
    private static final int PLOT_W = 340;
    private static final int PLOT_H = 280;
    private static final double X_MIN = 0.5;
    private static final double X_MAX = 1.0;
    private static final double Y_MIN = 0.2;
    private static final double Y_MAX = 1.0;

    public static String html(List<Calibration.Report> reports, String subtitle) {
        StringBuilder sb = new StringBuilder(16_000);
        sb.append("<!doctype html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n")
                .append("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n")
                .append("<title>Tennis price calibration</title>\n<style>\n").append(CSS).append("</style>\n</head>\n<body>\n")
                .append("<main>\n<h1>Are Polymarket tennis prices calibrated?</h1>\n")
                .append("<p class=\"sub\">").append(esc(subtitle)).append("</p>\n")
                .append("<p class=\"how\">Each dot is a price bucket for the market's favorite. Across: the average price. ")
                .append("Up: how often the favorite won. The gray line is perfect calibration, ")
                .append("the bars are 95% ranges, and bigger dots hold more matches.</p>\n")
                .append("<div class=\"grid\">\n");
        for (Calibration.Report r : reports) {
            panel(sb, r);
        }
        sb.append("</div>\n</main>\n<div id=\"tip\" role=\"tooltip\" hidden></div>\n<script>\n").append(JS)
                .append("</script>\n</body>\n</html>\n");
        return sb.toString();
    }

    private static void panel(StringBuilder sb, Calibration.Report r) {
        sb.append("<section class=\"panel\">\n<h2>")
                .append(r.minutesBeforeStart() > 0
                        ? r.minutesBeforeStart() + " minutes before the scheduled start"
                        : "At the scheduled start")
                .append("</h2>\n");
        if (r.n() == 0) {
            sb.append("<p class=\"note\">No matches had a price at this time.</p>\n</section>\n");
            return;
        }
        sb.append("<p class=\"stats\">").append(r.n()).append(" matches &middot; Brier ")
                .append(f(r.brier(), 3)).append(" (50/50 would be 0.250) &middot; log loss ")
                .append(f(r.logLoss(), 3)).append("<br>Favorites were priced ")
                .append(pct(r.meanFavoritePrice())).append(" on average and won ")
                .append(pct(r.favoriteWinRate())).append("</p>\n");
        int w = LEFT + PLOT_W + RIGHT;
        int h = TOP + PLOT_H + BOTTOM;
        sb.append("<svg viewBox=\"0 0 ").append(w).append(' ').append(h).append("\" role=\"img\" aria-label=\"")
                .append("Reliability chart, ").append(r.minutesBeforeStart()).append(" minutes before start\">\n");

        // Gridlines and tick labels.
        for (int t = 50; t <= 100; t += 10) {
            double x = xPix(t / 100.0);
            sb.append(line(x, TOP, x, TOP + PLOT_H, "gl"));
            sb.append(text(x, TOP + PLOT_H + 18, t + "%", "tick", "middle"));
        }
        for (int t = 20; t <= 100; t += 20) {
            double y = yPix(t / 100.0);
            sb.append(line(LEFT, y, LEFT + PLOT_W, y, "gl"));
            sb.append(text(LEFT - 8, y + 4, t + "%", "tick", "end"));
        }
        sb.append(text(LEFT + PLOT_W / 2.0, h - 6, "Market price of the favorite", "axis", "middle"));
        sb.append("<text class=\"axis\" text-anchor=\"middle\" transform=\"translate(14 ")
                .append(f(TOP + PLOT_H / 2.0, 1)).append(") rotate(-90)\">How often the favorite won</text>\n");

        // Perfect calibration.
        sb.append(line(xPix(0.5), yPix(0.5), xPix(1.0), yPix(1.0), "diag"));

        // Buckets: bars first, dots on top.
        for (Calibration.Bucket b : r.buckets()) {
            double x = xPix(b.meanPrice());
            sb.append(line(x, yPix(clampY(b.ciHigh())), x, yPix(clampY(b.ciLow())), "ci"));
        }
        for (Calibration.Bucket b : r.buckets()) {
            double x = xPix(b.meanPrice());
            double y = yPix(clampY(b.winRate()));
            double radius = Math.min(10, 4 + Math.sqrt(b.n()) * 0.6);
            String tip = String.format(Locale.ROOT,
                    "%.0f-%.0f%% bucket: %d matches, favorite won %d (%.1f%%), average price %.1f%%, 95%% range %.0f-%.0f%%",
                    b.low() * 100, b.high() * 100, b.n(), b.wins(), b.winRate() * 100,
                    b.meanPrice() * 100, b.ciLow() * 100, b.ciHigh() * 100);
            sb.append("<g class=\"pt\" tabindex=\"0\" data-tip=\"").append(esc(tip)).append("\">")
                    .append("<circle cx=\"").append(f(x, 1)).append("\" cy=\"").append(f(y, 1)).append("\" r=\"14\" class=\"hit\"/>")
                    .append("<circle cx=\"").append(f(x, 1)).append("\" cy=\"").append(f(y, 1)).append("\" r=\"")
                    .append(f(radius, 1)).append("\" class=\"dot\"/></g>\n");
        }
        sb.append("</svg>\n");

        // Table view of the same numbers.
        sb.append("<table>\n<thead><tr><th>Price bucket</th><th>Matches</th><th>Avg price</th>")
                .append("<th>Favorite won</th><th>95% range</th></tr></thead>\n<tbody>\n");
        for (Calibration.Bucket b : r.buckets()) {
            sb.append("<tr><td>").append(String.format(Locale.ROOT, "%.0f-%.0f%%", b.low() * 100, b.high() * 100))
                    .append("</td><td>").append(b.n()).append("</td><td>").append(pct(b.meanPrice()))
                    .append("</td><td>").append(pct(b.winRate())).append("</td><td>")
                    .append(String.format(Locale.ROOT, "%.0f-%.0f%%", b.ciLow() * 100, b.ciHigh() * 100))
                    .append("</td></tr>\n");
        }
        sb.append("</tbody>\n</table>\n");
        if (r.minutesBeforeStart() <= 0) {
            sb.append("<p class=\"note\">Matches often start later than scheduled, but some start earlier, ")
                    .append("so this snapshot can include play in progress.</p>\n");
        }
        sb.append("</section>\n");
    }

    private static double xPix(double p) {
        return LEFT + (p - X_MIN) / (X_MAX - X_MIN) * PLOT_W;
    }

    private static double yPix(double r) {
        return TOP + (1 - (r - Y_MIN) / (Y_MAX - Y_MIN)) * PLOT_H;
    }

    private static double clampY(double v) {
        return Math.max(Y_MIN, Math.min(Y_MAX, v));
    }

    private static String line(double x1, double y1, double x2, double y2, String cls) {
        return "<line x1=\"" + f(x1, 1) + "\" y1=\"" + f(y1, 1) + "\" x2=\"" + f(x2, 1) + "\" y2=\"" + f(y2, 1)
                + "\" class=\"" + cls + "\"/>\n";
    }

    private static String text(double x, double y, String s, String cls, String anchor) {
        return "<text x=\"" + f(x, 1) + "\" y=\"" + f(y, 1) + "\" class=\"" + cls + "\" text-anchor=\"" + anchor
                + "\">" + esc(s) + "</text>\n";
    }

    private static String f(double v, int decimals) {
        return String.format(Locale.ROOT, "%." + decimals + "f", v);
    }

    private static String pct(double v) {
        return String.format(Locale.ROOT, "%.1f%%", v * 100);
    }

    static String esc(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }

    private static final String CSS = String.join("\n",
            ":root { color-scheme: light; --surface:#fcfcfb; --ink:#0b0b0b; --ink2:#52514e; --muted:#6b6a66;",
            "  --grid:#e7e6e2; --series:#2a78d6; --diag:#8d8c86; }",
            "@media (prefers-color-scheme: dark) { :root:where(:not([data-theme=\"light\"])) { color-scheme: dark;",
            "  --surface:#1a1a19; --ink:#f0efec; --ink2:#c3c2b7; --muted:#a8a79e; --grid:#2f2f2d; --series:#3987e5; --diag:#8d8c86; } }",
            ":root[data-theme=\"dark\"] { color-scheme: dark; --surface:#1a1a19; --ink:#f0efec; --ink2:#c3c2b7;",
            "  --muted:#a8a79e; --grid:#2f2f2d; --series:#3987e5; --diag:#8d8c86; }",
            "* { box-sizing: border-box; }",
            "body { margin:0; background:var(--surface); color:var(--ink); font:15px/1.5 system-ui, -apple-system, 'Segoe UI', sans-serif; }",
            "main { max-width:1280px; margin:0 auto; padding:24px 16px 48px; }",
            "h1 { font-size:24px; margin:0 0 4px; }",
            "h2 { font-size:16px; margin:0 0 4px; }",
            ".sub, .how { color:var(--ink2); margin:0 0 8px; max-width:70ch; }",
            ".grid { display:grid; grid-template-columns:repeat(auto-fit, minmax(340px, 1fr)); gap:32px; margin-top:20px; }",
            ".panel { min-width:0; }",
            ".stats { margin:0 0 8px; color:var(--ink2); font-size:14px; }",
            ".note { margin:0 0 8px; color:var(--muted); font-size:13px; }",
            "svg { width:100%; height:auto; display:block; }",
            "line.gl { stroke:var(--grid); stroke-width:1; }",
            "line.diag { stroke:var(--diag); stroke-width:1.5; }",
            "line.ci { stroke:var(--series); stroke-width:2; opacity:.55; stroke-linecap:round; }",
            "circle.dot { fill:var(--series); stroke:var(--surface); stroke-width:2; }",
            "circle.hit { fill:transparent; }",
            ".pt { cursor:default; outline:none; }",
            ".pt:focus circle.dot, .pt:hover circle.dot { stroke:var(--ink); }",
            "text.tick { fill:var(--muted); font-size:11px; }",
            "text.axis { fill:var(--ink2); font-size:12px; }",
            "table { border-collapse:collapse; width:100%; margin-top:8px; font-size:13px; font-variant-numeric:tabular-nums; }",
            "th, td { text-align:right; padding:3px 6px; border-bottom:1px solid var(--grid); }",
            "th:first-child, td:first-child { text-align:left; }",
            "th { color:var(--ink2); font-weight:600; }",
            "#tip { position:absolute; z-index:10; max-width:280px; padding:6px 10px; font-size:13px; pointer-events:none;",
            "  background:var(--ink); color:var(--surface); border-radius:6px; transform:translate(-50%,-100%); }",
            "#tip[hidden] { display:none; }",
            "");

    private static final String JS = String.join("\n",
            "const tip = document.getElementById('tip');",
            "document.querySelectorAll('.pt').forEach(function (g) {",
            "  function show() {",
            "    tip.textContent = g.dataset.tip; tip.hidden = false;",
            "    const r = g.getBoundingClientRect();",
            "    tip.style.left = (window.scrollX + r.left + r.width / 2) + 'px';",
            "    tip.style.top = (window.scrollY + r.top - 6) + 'px';",
            "  }",
            "  function hide() { tip.hidden = true; }",
            "  g.addEventListener('mouseenter', show); g.addEventListener('focus', show);",
            "  g.addEventListener('mouseleave', hide); g.addEventListener('blur', hide);",
            "});",
            "");
}
