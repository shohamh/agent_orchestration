(() => {
  let current = null;

  async function check() {
    try {
      const response = await fetch("/api/dev/revision", { cache: "no-store" });
      if (!response.ok) throw new Error("Development reload endpoint unavailable");
      const { revision } = await response.json();
      if (current !== null && revision !== current) {
        location.reload();
        return;
      }
      current = revision;
    } catch {
      // Keep trying while the development server is restarting.
    }
    setTimeout(check, 1000);
  }

  check();
})();
