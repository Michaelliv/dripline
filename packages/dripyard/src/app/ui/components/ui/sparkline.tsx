/**
 * Tiny inline-SVG sparkline. No external deps.
 * Renders a polyline over normalized values with an optional area fill.
 */

interface SparklineProps {
  values: number[];
  width?: number;
  height?: number;
  stroke?: string;
  fill?: string;
  className?: string;
  /** If set, fixes the y-axis lower bound. Otherwise uses min(values). */
  yMin?: number;
  /** If set, fixes the y-axis upper bound. Otherwise uses max(values). */
  yMax?: number;
}

export function Sparkline({
  values,
  width = 80,
  height = 24,
  stroke = "currentColor",
  fill,
  className,
  yMin,
  yMax,
}: SparklineProps) {
  if (values.length === 0) {
    return (
      <svg
        width={width}
        height={height}
        className={className}
        aria-hidden="true"
      />
    );
  }

  const min = yMin ?? Math.min(...values);
  const max = yMax ?? Math.max(...values);
  const range = max - min || 1; // avoid divide-by-zero on flat series

  const pts = values.map((v, i) => {
    const x = (i / Math.max(values.length - 1, 1)) * width;
    const y = height - ((v - min) / range) * height;
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });

  const polyline = pts.join(" ");
  const area = fill
    ? `${pts[0].split(",")[0]},${height} ${polyline} ${pts[pts.length - 1].split(",")[0]},${height}`
    : null;

  return (
    <svg
      width={width}
      height={height}
      className={className}
      aria-hidden="true"
    >
      {area && <polyline points={area} fill={fill} stroke="none" />}
      <polyline
        points={polyline}
        fill="none"
        stroke={stroke}
        strokeWidth={1.25}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
