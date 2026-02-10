import { test, expect } from "@playwright/test";
import { mockAllApiRoutes, navigateAndWait } from "./helpers";

test.describe("ChunkList", () => {
  test("renders chunk section header", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    await expect(page.getByRole("heading", { name: "Chunks" })).toBeVisible();
  });

  test("displays chunk statistics", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Stats line: "Total: 3 | Pending: 1 | Sent: 1 | In Process: 1"
    await expect(page.getByText("Total: 3")).toBeVisible();
    await expect(page.getByText("Pending: 1")).toBeVisible();
    await expect(page.getByText("Sent: 1")).toBeVisible();
    await expect(page.getByText("In Process: 1")).toBeVisible();
  });

  test("renders chunk table with headers", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Table headers
    await expect(page.getByRole("columnheader", { name: "ID" })).toBeVisible();
    await expect(
      page.getByRole("columnheader", { name: "Size" }),
    ).toBeVisible();
    await expect(page.getByRole("columnheader", { name: "MD5" })).toBeVisible();
    await expect(
      page.getByRole("columnheader", { name: "Status" }),
    ).toBeVisible();
    await expect(
      page.getByRole("columnheader", { name: "Created" }),
    ).toBeVisible();
  });

  test("displays chunk rows with correct data", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Wait for chunks to load
    await expect(page.getByText("1048576")).toBeVisible();

    // Check chunk IDs visible
    const cells = page.locator("td");
    const cellTexts = await cells.allTextContents();

    // Should have 3 rows x 5 columns = 15 cells
    expect(cellTexts.length).toBe(15);

    // First chunk ID
    expect(cellTexts[0]).toBe("1");
    // First chunk size
    expect(cellTexts[1]).toBe("1048576");
  });

  test("shows correct chunk statuses", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // Wait for chunks to render
    await expect(page.getByText("Sent").first()).toBeVisible();
    await expect(page.getByText("Uploading")).toBeVisible();
    await expect(page.getByText("Pending").first()).toBeVisible();
  });

  test("shows truncated MD5 hashes", async ({ page }) => {
    await mockAllApiRoutes(page);
    await navigateAndWait(page);

    // MD5 is truncated to first 8 chars + "..."
    await expect(page.getByText("d41d8cd9...")).toBeVisible();
    await expect(page.getByText("098f6bcd...")).toBeVisible();
  });

  test("renders empty table when no chunks", async ({ page }) => {
    await mockAllApiRoutes(page, {
      chunks: [],
      chunkStats: {
        total_chunks: 0,
        pending_chunks: 0,
        sent_chunks: 0,
        in_process_chunks: 0,
        total_bytes: 0,
        buffer_duration_secs: 0,
      },
    });
    await navigateAndWait(page);

    // Stats show zeros
    await expect(page.getByText("Total: 0")).toBeVisible();

    // Table exists but has no data rows
    const rows = page.locator("tbody tr");
    await expect(rows).toHaveCount(0);
  });
});
