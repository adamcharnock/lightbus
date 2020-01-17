window.addEventListener("DOMContentLoaded", function() {
  function normalizePath(path) {
    var normalized = [];
    path.split("/").forEach(function(bit, i) {
      if (bit === "." || (bit === "" && i !== 0)) {
        return;
      } else if (bit === "..") {
        if (normalized.length === 1 && normalized[0] === "") {
          // We must be trying to .. past the root!
          throw new Error("invalid path");
        } else if (normalized.length === 0 ||
                   normalized[normalized.length - 1] === "..") {
          normalized.push("..");
        } else {
          normalized.pop();
        }
      } else {
        normalized.push(bit);
      }
    });
    return normalized.join("/");
  }

  // `base_url` comes from the base.html template for this theme.
  // Lightbus note: The base_url js variable wasn't immediately obviously
  //                available on the mkdocs-material theme. Hard coding
  //                here for Lightbus' docs.
  var REL_BASE_URL = '';
  var ABS_BASE_URL = normalizePath(window.location.pathname + "/" +
                                   REL_BASE_URL);
  var CURRENT_VERSION = ABS_BASE_URL.split("/").pop();

  function makeSelect(options, selected) {
    var select = document.createElement("select");

    options.forEach(function(i) {
      var option = new Option(i.text, i.value, undefined,
                              i.value === selected);
      select.add(option);
    });

    return select;
  }

  var xhr = new XMLHttpRequest();
  xhr.open("GET", REL_BASE_URL + "/../versions.json");
  xhr.onload = function() {
    var versions = JSON.parse(this.responseText);

    var realVersion = versions.find(function(i) {
      return i.version === CURRENT_VERSION ||
             i.aliases.includes(CURRENT_VERSION);
    }).version;

    var select = makeSelect(versions.map(function(i) {
      return {text: i.title, value: i.version};
    }), realVersion);
    select.id = "version-selector";
    select.addEventListener("change", function(event) {
      window.location.href = REL_BASE_URL + "/../" + this.value;
    });

    var selectInUl = document.createElement('ul');
    selectInUl.appendChild(select);
    selectInUl.classList.add('md-tabs__list');
    var tabs = document.querySelector("nav.md-tabs > .md-tabs__inner");
    tabs.appendChild(selectInUl);
  };
  xhr.send();
});
