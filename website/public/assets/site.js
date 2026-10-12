(function () {
  var KEY = "mdbook-theme";
  var root = document.documentElement;
  var darkQuery = window.matchMedia("(prefers-color-scheme: dark)");
  var DARK_CLASSES = ["coal", "navy", "ayu"];

  function stored() {
    try {
      return localStorage.getItem(KEY);
    } catch (e) {
      return null;
    }
  }

  function resolve() {
    var saved = stored();
    if (saved) {
      return DARK_CLASSES.indexOf(saved) !== -1 ? "dark" : "light";
    }
    return darkQuery.matches ? "dark" : "light";
  }

  function apply(mode) {
    root.setAttribute("data-theme", mode);
    root.classList.remove("light", "rust", "coal", "navy", "ayu");
    root.classList.add(mode === "dark" ? "navy" : "light");
    var buttons = document.querySelectorAll("[data-theme-toggle]");
    for (var i = 0; i < buttons.length; i++) {
      var next = mode === "dark" ? "light" : "dark";
      buttons[i].textContent = mode === "dark" ? "Light" : "Dark";
      buttons[i].setAttribute("aria-label", "Switch to " + next + " theme");
    }
  }

  apply(resolve());

  darkQuery.addEventListener("change", function () {
    if (!stored()) {
      apply(resolve());
    }
  });

  document.addEventListener("click", function (event) {
    var button = event.target.closest && event.target.closest("[data-theme-toggle]");
    if (!button) {
      return;
    }
    var next = resolve() === "dark" ? "light" : "dark";
    try {
      localStorage.setItem(KEY, next === "dark" ? "navy" : "light");
    } catch (e) {}
    apply(next);
  });

  document.addEventListener("DOMContentLoaded", function () {
    var buttons = document.querySelectorAll("[data-theme-toggle]");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].hidden = false;
    }
    apply(resolve());
  });
})();
