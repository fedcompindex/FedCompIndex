// FedComp Index - main.js

(function () {
  // ─── Mobile nav toggle ──────────────────────────────────────────────
  var toggle = document.getElementById('nav-toggle');
  var links = document.getElementById('nav-links');
  if (toggle && links) {
    toggle.addEventListener('click', function () {
      links.classList.toggle('open');
    });
  }

  // ─── Table filter ────────────────────────────────────────────────────
  var filterInput = document.getElementById('filter');
  if (filterInput) {
    filterInput.addEventListener('input', function () {
      var q = this.value.toLowerCase();
      var rows = document.querySelectorAll('#main-table tbody tr');
      var count = 0;
      rows.forEach(function (row) {
        var match = row.textContent.toLowerCase().indexOf(q) !== -1;
        row.style.display = match ? '' : 'none';
        if (match) count++;
      });
      var countEl = document.getElementById('filter-count');
      if (countEl) countEl.textContent = q ? count + ' results' : '';
    });
  }

  // ─── Table sort ──────────────────────────────────────────────────────
  var table = document.getElementById('main-table');
  if (table) {
    var sortState = { col: -1, dir: 1 };
    var headers = table.querySelectorAll('th.sort');
    var tbody = table.querySelector('tbody');

    headers.forEach(function (th, i) {
      th.addEventListener('click', function () {
        var col = parseInt(this.dataset.col);
        var dir = sortState.col === col ? -sortState.dir : -1; // default desc
        sortState = { col: col, dir: dir };

        headers.forEach(function (h) { h.classList.remove('asc', 'desc'); });
        th.classList.add(dir === 1 ? 'asc' : 'desc');

        var rows = Array.from(tbody.querySelectorAll('tr'));
        rows.sort(function (a, b) {
          var aText = a.cells[col] ? a.cells[col].textContent.trim() : '';
          var bText = b.cells[col] ? b.cells[col].textContent.trim() : '';
          var aNum = parseFloat(aText.replace(/[^0-9.\-]/g, ''));
          var bNum = parseFloat(bText.replace(/[^0-9.\-]/g, ''));
          if (!isNaN(aNum) && !isNaN(bNum)) return (aNum - bNum) * dir;
          return aText.localeCompare(bText) * dir;
        });
        rows.forEach(function (r) { tbody.appendChild(r); });
      });
    });
  }
  // ─── Nav search ──────────────────────────────────────────────────────
  var navSearch = document.getElementById('nav-search');
  var navResults = document.getElementById('nav-search-results');
  var searchIndex = null;

  if (navSearch && navResults) {
    navSearch.addEventListener('focus', function () {
      if (!searchIndex) {
        fetch('/static/search.json')
          .then(function (r) { return r.json(); })
          .then(function (data) { searchIndex = data; })
          .catch(function () { searchIndex = []; });
      }
    });

    navSearch.addEventListener('input', function () {
      var q = this.value.toLowerCase().trim();
      if (!q || !searchIndex) {
        navResults.classList.remove('open');
        return;
      }
      var matches = [];
      for (var i = 0; i < searchIndex.length && matches.length < 8; i++) {
        var c = searchIndex[i];
        if (c.n.toLowerCase().indexOf(q) !== -1 || c.ca.toLowerCase().indexOf(q) !== -1) {
          matches.push(c);
        }
      }
      if (!matches.length) {
        navResults.classList.remove('open');
        return;
      }
      navResults.innerHTML = matches.map(function (m) {
        return '<a href="/dossier/' + m.s + '/"><span>' + m.n + '</span><span class="sr-score">' + m.sc + ' - ' + m.cl + '</span></a>';
      }).join('');
      navResults.classList.add('open');
    });

    document.addEventListener('click', function (e) {
      if (!navSearch.contains(e.target) && !navResults.contains(e.target)) {
        navResults.classList.remove('open');
      }
    });
  }

  // ─── State chart (live) ──────────────────────────────────────────────────
  var cc = document.getElementById('state-chart');
  if (cc) {
    var cx = cc.getContext('2d');
    var chartData = null;
    var MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

    function dpr() { return window.devicePixelRatio || 1; }

    function sizeCanvas() {
      var r = dpr();
      var cw = cc.parentElement.clientWidth;
      cc.width = cw * r;
      cc.height = 160 * r;
      cc.style.width = cw + 'px';
      cc.style.height = '160px';
      cx.scale(r, r);
    }

    function drawLoading(msg) {
      sizeCanvas();
      var w = cc.width / dpr();
      cx.fillStyle = '#4B5563';
      cx.font = '12px Inter, ui-sans-serif, system-ui, sans-serif';
      cx.textAlign = 'center';
      cx.fillText(msg, w / 2, 86);
    }

    function redraw() {
      if (!chartData) return;
      sizeCanvas();
      var r = dpr();
      var w = cc.width / r;
      var h = 160;
      var slice = chartData.slice(-12);
      var n = slice.length;
      if (!n) return;

      var pL = 8, pR = 8, pT = 24, pB = 22;
      var cw = w - pL - pR;
      var ch = h - pT - pB;
      var bw = cw / n;
      var maxV = Math.max.apply(null, slice.map(function (d) { return d.m; }));

      // Grid lines
      cx.strokeStyle = '#1a2540';
      cx.lineWidth = 1;
      [0.33, 0.66, 1].forEach(function (f) {
        var y = pT + ch * (1 - f);
        cx.beginPath(); cx.moveTo(pL, y); cx.lineTo(w - pR, y); cx.stroke();
      });

      // Peak label
      cx.fillStyle = '#4B5563';
      cx.font = '10px Inter, ui-sans-serif, system-ui, sans-serif';
      cx.textAlign = 'left';
      var peak = maxV >= 1000 ? '$' + (maxV / 1000).toFixed(1) + 'B' : '$' + Math.round(maxV) + 'M';
      cx.fillText(peak, pL, pT - 7);

      slice.forEach(function (d, i) {
        var barH = maxV > 0 ? (d.m / maxV) * ch : 0;
        var x = pL + i * bw + 2;
        var bwi = bw - 4;
        var barY = pT + ch - barH;

        cx.fillStyle = '#2563EB';
        if (barH > 0) cx.fillRect(x, barY, bwi, barH);

        // X label: show month, add year when Jan or first bar
        var showLabel = n <= 14 || i % Math.ceil(n / 14) === 0;
        if (showLabel) {
          cx.fillStyle = '#64748B';
          cx.font = '9px Inter, ui-sans-serif, system-ui, sans-serif';
          cx.textAlign = 'center';
          var lbl = MONTHS[d.mo - 1];
          if (i === 0 || d.mo === 1) lbl += ' \'' + String(d.yr).slice(2);
          cx.fillText(lbl, x + bwi / 2, h - 5);
        }

        // Value above bar if tall enough
        if (barH > 22) {
          cx.fillStyle = '#94A3B8';
          cx.font = '8px Inter, ui-sans-serif, system-ui, sans-serif';
          cx.textAlign = 'center';
          var v = d.m >= 1000 ? '$' + (d.m / 1000).toFixed(1) + 'B' : '$' + Math.round(d.m) + 'M';
          cx.fillText(v, x + bwi / 2, barY - 3);
        }
      });
    }

    drawLoading('Loading...');

    // Fetch 2 years of monthly NV award data from USASpending
    var now = new Date();
    var startD = new Date(now); startD.setMonth(startD.getMonth() - 13);
    fetch('https://api.usaspending.gov/api/v2/search/spending_over_time/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        group: 'month',
        filters: {
          time_period: [{ start_date: startD.toISOString().slice(0, 10), end_date: now.toISOString().slice(0, 10) }],
          recipient_locations: [{ country: 'USA', state: window.__STATE_CODE__ || 'NV' }],
          award_type_codes: ['A', 'B', 'C', 'D']
        }
      })
    })
    .then(function (r) { return r.json(); })
    .then(function (resp) {
      chartData = (resp.results || []).map(function (r) {
        var fmo = parseInt(r.time_period.month); // fiscal month: 1=Oct, 2=Nov, 3=Dec, 4=Jan...
        var fy = parseInt(r.time_period.fiscal_year);
        var calMo = fmo <= 3 ? fmo + 9 : fmo - 3;
        var calYr = fmo <= 3 ? fy - 1 : fy;
        return { yr: calYr, mo: calMo, m: (r.aggregated_amount || 0) / 1e6 };
      }).sort(function (a, b) {
        return (a.yr * 12 + a.mo) - (b.yr * 12 + b.mo);
      });

      var updated = document.getElementById('chart-updated');
      if (updated && chartData.length) {
        var last = chartData[chartData.length - 1];
        updated.textContent = 'through ' + MONTHS[last.mo - 1] + ' ' + last.yr;
      }

      redraw();
      window.addEventListener('resize', redraw);
    })
    .catch(function () { drawLoading('Data unavailable'); });

  }

  // ─── Recent awards (live) ─────────────────────────────────────────────────
  var raEl = document.getElementById('recent-awards');
  if (raEl && window.__STATE_CODE__) {
    var ueiIdx = null;
    var MONTHS_SHORT = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];

    function fmtVal(v) {
      if (!v) return '-';
      if (v >= 1e9) return '$' + (v/1e9).toFixed(1) + 'B';
      if (v >= 1e6) return '$' + (v/1e6).toFixed(1) + 'M';
      if (v >= 1e3) return '$' + Math.round(v/1e3) + 'K';
      return '$' + Math.round(v);
    }

    function fmtDate(s) {
      if (!s) return '-';
      var d = new Date(s);
      return MONTHS_SHORT[d.getUTCMonth()] + ' ' + d.getUTCDate();
    }

    function renderAwards(results) {
      if (!results || !results.length) {
        raEl.innerHTML = '<div class="empty">No recent awards found.</div>';
        return;
      }
      var rows = results.map(function(a) {
        var uei = a['Recipient UEI'] || '';
        var scoreCell = '-';
        if (ueiIdx && ueiIdx[uei]) {
          var entry = ueiIdx[uei];
          var cls = entry.score >= 75 ? 'good' : entry.score >= 50 ? 'warn' : 'bad';
          scoreCell = '<a href="/dossier/' + entry.slug + '/"><span class="badge ' + cls + '">' + entry.score + '</span></a>';
        }
        var psc = a['PSC'];
        if (psc && typeof psc === 'object') psc = psc.code || '-';
        var agency = (a['Awarding Agency'] || '-').replace('Department of ', 'DoD - ').slice(0, 32);
        return '<tr>'
          + '<td class="muted" style="white-space:nowrap;">' + fmtDate(a['Start Date']) + '</td>'
          + '<td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:var(--sz-label);">' + agency + '</td>'
          + '<td class="muted">' + (psc || '-') + '</td>'
          + '<td style="text-align:right;">' + fmtVal(a['Award Amount']) + '</td>'
          + '<td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">' + (a['Recipient Name'] || '-') + '</td>'
          + '<td style="text-align:right;">' + scoreCell + '</td>'
          + '</tr>';
      }).join('');

      raEl.innerHTML = '<div class="table-wrap"><table>'
        + '<thead><tr>'
        + '<th>Date</th><th>Agency</th><th>PSC</th>'
        + '<th style="text-align:right;">Value</th>'
        + '<th>Recipient</th>'
        + '<th style="text-align:right;" title="FedComp Index score">Index</th>'
        + '</tr></thead>'
        + '<tbody>' + rows + '</tbody>'
        + '</table></div>';
    }

    var now2 = new Date();
    var start2 = new Date(now2); start2.setMonth(start2.getMonth() - 3);
    fetch('https://api.usaspending.gov/api/v2/search/spending_by_award/', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filters: {
          time_period: [{ start_date: start2.toISOString().slice(0, 10), end_date: now2.toISOString().slice(0, 10) }],
          award_type_codes: ['A', 'B', 'C', 'D'],
          recipient_locations: [{ country: 'USA', state: window.__STATE_CODE__ }]
        },
        fields: ['Award ID','Recipient Name','Recipient UEI','Award Amount','Start Date','Awarding Agency','PSC'],
        page: 1, limit: 15, sort: 'Start Date', order: 'desc', spending_level: 'awards'
      })
    })
    .then(function(r) { return r.json(); })
    .then(function(resp) {
      var today = new Date().toISOString().slice(0, 10);
      var results = (resp.results || [])
        .filter(function(a) { return a['Start Date'] && a['Start Date'] <= today; })
        .sort(function(a, b) { return a['Start Date'] < b['Start Date'] ? 1 : -1; });
      fetch('/static/uei_index.json')
        .then(function(r) { return r.json(); })
        .then(function(idx) { ueiIdx = idx; renderAwards(results); })
        .catch(function() { renderAwards(results); });
    })
    .catch(function() {
      raEl.innerHTML = '<div class="empty">Could not load recent awards.</div>';
    });
  }
})();
