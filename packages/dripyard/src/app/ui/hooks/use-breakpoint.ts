import { useEffect, useState } from "react";

const BREAKPOINTS = {
  wide: 1280,
} as const;

export type Breakpoint = "default" | "wide";

export interface BreakpointInfo {
  /** Current breakpoint name */
  breakpoint: Breakpoint;
  /** Window width in px */
  width: number;
  /** True when window is >= 1280px */
  isWide: boolean;
}

export function useBreakpoint(): BreakpointInfo {
  const [width, setWidth] = useState(window.innerWidth);

  useEffect(() => {
    const onResize = () => setWidth(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  const isWide = width >= BREAKPOINTS.wide;

  return {
    breakpoint: isWide ? "wide" : "default",
    width,
    isWide,
  };
}
