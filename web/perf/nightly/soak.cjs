// The 10-minute soak, a memlab scenario: memlab snapshots the app before and after ten
// minutes open and reports what it leaked.
module.exports = {
  url: () => "http://localhost:4173/",
  action: () => new Promise((resolve) => setTimeout(resolve, 10 * 60_000)),
  back: async () => {},
};
