import {
  createContext,
  type ReactNode,
  type RefObject,
  useContext,
  useEffect,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { ArrowLeft } from "lucide-react";
import { Link } from "react-router-dom";

/**
 * PageHeader portals a consistent title / description / actions bar
 * into the fixed app top bar. Every page calls it the same way; the
 * top bar is the single visual home for page identity, so scrolling
 * body content never hides the title and every page feels uniform.
 *
 * Usage:
 *   <PageHeader
 *     title="Lanes"
 *     description="5 configured · dripline schedule"
 *     actions={<Button>…</Button>}
 *     back={{ to: "/runs", label: "All runs" }}
 *   />
 *
 * Each page body starts with content (table, cards, editor); never
 * another h2 for the page title.
 */

const PageHeaderCtx = createContext<RefObject<HTMLDivElement | null>>({
  current: null,
});

export function PageHeaderProvider({ children }: { children: ReactNode }) {
  const ref = useRef<HTMLDivElement>(null);
  return (
    <PageHeaderCtx.Provider value={ref}>{children}</PageHeaderCtx.Provider>
  );
}

export function PageHeaderSlot() {
  const ref = useContext(PageHeaderCtx);
  return (
    <div
      ref={ref}
      className="flex items-center gap-3 flex-1 min-w-0 h-full"
    />
  );
}

export interface PageHeaderProps {
  title: ReactNode;
  description?: ReactNode;
  /** Optional back link — renders as an inline chevron + label before the title. */
  back?: { to: string; label: string };
  /** Right-aligned action area (buttons, inputs). */
  actions?: ReactNode;
}

export function PageHeader({
  title,
  description,
  back,
  actions,
}: PageHeaderProps) {
  const ref = useContext(PageHeaderCtx);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
    return () => setMounted(false);
  }, []);

  if (!mounted || !ref.current) return null;

  return createPortal(
    <div className="flex items-center gap-3 flex-1 min-w-0 h-full">
      {back && (
        <Link
          to={back.to}
          className="flex items-center gap-1 text-xs text-shift-muted hover:text-shift-text flex-none"
          title={`Back to ${back.label}`}
        >
          <ArrowLeft className="size-3" />
          {back.label}
        </Link>
      )}
      <div className="flex items-baseline gap-2 min-w-0 flex-1">
        <h1 className="text-sm font-semibold truncate">{title}</h1>
        {description && (
          <span className="text-xs text-shift-muted truncate">
            {description}
          </span>
        )}
      </div>
      {actions && (
        <div className="flex items-center gap-2 flex-none">{actions}</div>
      )}
    </div>,
    ref.current,
  );
}
