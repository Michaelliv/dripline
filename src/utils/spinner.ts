const FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

export function startSpinner(message: string): { stop: (finalMessage?: string) => void } {
  if (!process.stderr.isTTY) {
    return { stop: () => {} };
  }

  let i = 0;
  const interval = setInterval(() => {
    process.stderr.write(`\r${FRAMES[i % FRAMES.length]} ${message}`);
    i++;
  }, 80);

  return {
    stop(finalMessage?: string) {
      clearInterval(interval);
      process.stderr.write(`\r${" ".repeat(message.length + 3)}\r`);
      if (finalMessage) {
        process.stderr.write(`${finalMessage}\n`);
      }
    },
  };
}
