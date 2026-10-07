// Small progressive-enhancement helpers. The UI works without JavaScript; nothing here is
// security-relevant (authorization and CSRF are enforced server-side).
(function () {
  "use strict";

  // Confirmation for destructive actions (STD-005).
  document.addEventListener("submit", function (e) {
    var form = e.target;
    var msg = form.getAttribute("data-confirm");
    if (msg && !window.confirm(msg)) {
      e.preventDefault();
      e.stopImmediatePropagation();
      return;
    }
    // Disabled/loading state while submitting.
    form.querySelectorAll("[data-disable-on-submit]").forEach(function (b) {
      setTimeout(function () { b.disabled = true; b.textContent = "Working…"; }, 0);
    });
  }, true);

  function setup() {
    // FD-008: show the features of the selected plan.
    var plan = document.querySelector("select[data-plan-features]");
    if (plan && window.htmx) {
      var load = function () {
        var target = plan.getAttribute("data-plan-features");
        if (plan.value) {
          window.htmx.ajax("GET", "/admin/plans/" + encodeURIComponent(plan.value) + "/features", target);
        } else {
          document.querySelector(target).innerHTML = "";
        }
      };
      plan.addEventListener("change", load);
      if (plan.value) { load(); }
    }
    // FD-009: inheritance options only when a parent is chosen.
    var parent = document.querySelector("select[data-toggle-target]");
    if (parent) {
      var box = document.getElementById(parent.getAttribute("data-toggle-target"));
      var sync = function () { if (box) { box.hidden = !parent.value; } };
      parent.addEventListener("change", sync);
      sync();
    }
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", setup);
  } else {
    setup();
  }
})();
