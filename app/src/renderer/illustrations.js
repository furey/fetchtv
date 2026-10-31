'use strict'

const illustrations = {
  'no-box': {
    label: 'A Fetch box and a computer with no line between them',
    svg: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 160" width="320" height="160" class="ill" aria-hidden="true">' +
      '<rect x="0" y="0" width="320" height="160" rx="8" class="f-p"/>' +
      '<rect x="24" y="86" width="104" height="34" rx="6" class="f-k2"/>' +
      '<rect x="36" y="101" width="18" height="5" rx="2.5" class="f-k3"/>' +
      '<circle cx="114" cy="103" r="3.5" class="f-y"/>' +
      '<rect x="196" y="48" width="100" height="66" rx="6" class="f-k2"/>' +
      '<rect x="204" y="56" width="84" height="50" rx="3" class="f-b" opacity="0.35"/>' +
      '<rect x="184" y="114" width="124" height="10" rx="5" class="f-k1"/>' +
      '<path d="M128 96C142 80 150 76 160 74" fill="none" stroke-width="6" stroke-linecap="round" stroke-dasharray="2 12" class="s-ink"/>' +
      '<path d="M176 72C182 71 188 72 194 74" fill="none" stroke-width="6" stroke-linecap="round" stroke-dasharray="2 12" class="s-ink"/>' +
      '<circle cx="168" cy="60" r="11" class="f-k1"/>' +
      '<rect x="166" y="53" width="4" height="9" rx="2" class="f-o"/>' +
      '<circle cx="168" cy="66" r="2.2" class="f-o"/>' +
      '</svg>'
  },
  'no-recordings': {
    label: 'An empty shelf where recordings would be',
    svg: '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 320 160" width="320" height="160" class="ill" aria-hidden="true">' +
      '<rect x="0" y="0" width="320" height="160" rx="8" class="f-p"/>' +
      '<rect x="56" y="118" width="208" height="10" rx="5" class="f-k2"/>' +
      '<rect x="72" y="58" width="44" height="60" rx="5" fill="none" stroke-width="3" stroke-dasharray="6 6" class="s-k3"/>' +
      '<rect x="138" y="58" width="44" height="60" rx="5" fill="none" stroke-width="3" stroke-dasharray="6 6" class="s-k3"/>' +
      '<rect x="204" y="58" width="44" height="60" rx="5" fill="none" stroke-width="3" stroke-dasharray="6 6" class="s-k3"/>' +
      '<rect x="134" y="26" width="52" height="8" rx="4" class="f-b" opacity="0.8"/>' +
      '<rect x="146" y="38" width="28" height="8" rx="4" class="f-o" opacity="0.8"/>' +
      '</svg>'
  }
}

const illustration = (name) => {
  const figure = document.createElement('figure')
  figure.className = 'illustration'
  figure.setAttribute('role', 'img')
  figure.setAttribute('aria-label', illustrations[name].label)
  figure.innerHTML = illustrations[name].svg
  return figure
}
