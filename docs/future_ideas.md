# Project Ideas & TODO

This page contains design notes and ideas for future improvements to Satellite Fetcher.

---

## Planned / Proposed Features

- **Concurrent Downloads**  
  Use multiple sessions (async or threaded) to speed up large dataset downloads.
  - Employ Python asyncio/aiohttp or threading.
  - Use a semaphore (asyncio.Semaphore) to control the number of simultaneous downloads and avoid service overload or rate limits.

---

Add your ideas and feature proposals here!
