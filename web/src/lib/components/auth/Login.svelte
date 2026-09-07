<script lang="ts">
  import { onMount } from 'svelte';
  import type { SessionController } from '../../api/session.svelte';

  let { session }: { session: SessionController } = $props();
  let apiKey = $state('');
  let returnTo = $state('/');

  onMount(() => {
    returnTo = `${window.location.pathname}${window.location.search}${window.location.hash}`;
  });

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    await session.login(apiKey);
  }
</script>

<main class="login" aria-label="Authentication">
  {#if session.googleOIDCEnabled}
    <form aria-label="Google sign in" action="/auth/google/login" method="get">
      <p class="eyebrow">msgvault</p>
      <h1>Log in</h1>
      <p>Use the Google account allowed for this archive.</p>
      <input type="hidden" name="return_to" value={returnTo} />
      <button type="submit">Continue with Google</button>
    </form>
    <p aria-hidden="true">or use the recovery login</p>
  {/if}
  <form aria-label="Log in" onsubmit={submit}>
    {#if !session.googleOIDCEnabled}
      <p class="eyebrow">msgvault</p>
      <h1>Log in</h1>
    {/if}
    <p>Enter the API key configured for this daemon.</p>

    <label for="api-key">API key</label>
    <input
      id="api-key"
      name="api-key"
      type="password"
      autocomplete="current-password"
      bind:value={apiKey}
      required
    />

    {#if session.error}
      <p role="alert">{session.error}</p>
    {/if}

    <button type="submit" disabled={session.loading}>
      {session.loading ? 'Logging in…' : 'Log in'}
    </button>
  </form>
</main>
