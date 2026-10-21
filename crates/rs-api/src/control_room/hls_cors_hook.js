// #415: YouTube Studio's live preview in a control room.
//
// Chrome 151+ plays HLS natively and refuses media segments that come from
// another origin than the manifest unless the media element asked in CORS
// mode (media/filters/hls_network_access_impl.cc: "insecure media request").
// Studio hands its manifest (www.youtube.com/api/manifest/hls_*) to a <video>
// without a crossorigin attribute, and the segments come from *.c.youtube.com,
// so the preview never played (stream-pp, Chrome 155, 2026-10-21). YouTube
// answers CORS for Studio with credentials (Access-Control-Allow-Origin:
// https://studio.youtube.com, -Allow-Credentials: true), so such an element
// asks in "use-credentials" mode: the same cookies as before, now CORS-checked.
// Chromium's own retry in CORS mode (CL 8384822) makes this redundant once it
// ships.
(() => {
  if (window.__restreamerHlsCors) return;
  window.__restreamerHlsCors = true;
  if (!/(^|\.)youtube\.com$/.test(location.hostname)) return;
  const isHls = (url) => /\/api\/manifest\/hls_/.test(String(url));
  const mark = (el, url) => {
    if (isHls(url) && !el.hasAttribute('crossorigin')) el.crossOrigin = 'use-credentials';
  };
  const src = Object.getOwnPropertyDescriptor(HTMLMediaElement.prototype, 'src');
  Object.defineProperty(HTMLMediaElement.prototype, 'src', {
    ...src,
    set(url) {
      mark(this, url);
      src.set.call(this, url);
    },
  });
  const setAttribute = Element.prototype.setAttribute;
  Element.prototype.setAttribute = function (name, value) {
    if (this instanceof HTMLMediaElement && String(name).toLowerCase() === 'src') mark(this, value);
    return setAttribute.call(this, name, value);
  };
})();
