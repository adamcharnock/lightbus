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
  //                available on the mkdocs-material theme. So we were
  //                simply assume the first part of the URL is the version.
  var CURRENT_VERSION = window.location.pathname.split("/")[1];

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
  // Lightbus note: Again, we make assumptions about the path
  xhr.open("GET", "/versions.json");
  xhr.onload = function() {
    var versions = JSON.parse(this.responseText);

    var realVersion = versions.find(function(i) {
      return i.version === CURRENT_VERSION ||
             i.aliases.includes(CURRENT_VERSION);
    }).version;

    var select = makeSelect(versions.map(function(i) {
      return {text: i.title, value: i.version};
    }), realVersion);
    select.addEventListener("change", function(event) {
      window.location.href = "/" + this.value;
    });

    var selectInLi = document.createElement('li');
    selectInLi.appendChild(select);
    selectInLi.className = 'md-nav__item';
    selectInLi.id = 'version-selector';
    var primarySidebarUl = document.querySelector(".md-nav--primary > .md-nav__list");
    var secondarySidebarUl = document.querySelector(".md-nav--primary > .md-nav__list > .md-nav__item--active.md-nav__item--nested .md-nav__list");
    if(secondarySidebarUl) {
      secondarySidebarUl.appendChild(selectInLi);
    } else {
      primarySidebarUl.appendChild(selectInLi);
    }
  };
  xhr.send();
});
