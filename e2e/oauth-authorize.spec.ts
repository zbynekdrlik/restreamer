import { test, expect } from '@playwright/test';

// Run against the mock backend used by frontend.spec.ts.
// The backend exposes _test endpoints that let us pre-seed grant state.

test.describe('OAuth Authorize channel', () => {
  test('authorize new channel happy path', async ({ page }) => {
    const consoleMessages: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error' || msg.type() === 'warning') {
        consoleMessages.push(`[${msg.type()}] ${msg.text()}`);
      }
    });
    await page.goto('/');

    // Open the Channels panel + click Authorize.
    await page.getByRole('button', { name: 'Authorize new channel' }).click();

    // Fill the label and submit.
    await page.getByLabel('Channel label').fill('bb');
    await page.getByRole('button', { name: 'Start authorization' }).click();

    // user_code + verification_url visible.
    await expect(page.getByTestId('oauth-user-code')).toHaveText('AB-CD-12');
    await expect(page.getByTestId('oauth-verification-url')).toHaveAttribute(
      'href', /google\.com\/device/);

    // Mock backend transitions to granted (test fixture endpoint).
    await page.evaluate(async () => {
      await fetch('/api/v1/_test/oauth-device-grant', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ label: 'bb', channel_id: 'UCxxxxxxxx' }),
      });
    });

    // Modal closes, channel appears in the table.
    await expect(page.getByTestId('oauth-modal')).toBeHidden({ timeout: 10_000 });
    await expect(page.getByTestId('oauth-channel-row-bb')).toBeVisible();
    await expect(page.getByTestId('oauth-channel-row-bb')).toContainText('UCxxxxxxxx');

    // Zero console errors / warnings (per browser-console-zero-errors.md).
    // Ignore known-benign Chromium subresource-integrity preload warning.
    const real = consoleMessages.filter(
      (m) => !/integrity.*attribute.*currently ignored.*subresource integrity/i.test(m),
    );
    expect(real).toEqual([]);
  });
});
