# Presentation build

`deck.js` holds every slide as data (positions in inches, texts, speaker notes, planned minutes). `build.js` draws the same scenes twice: an editable PowerPoint file and an interactive HTML presentation.

```powershell
cd scripts\presentation_build
npm install                 # one time, installs pptxgenjs
node build.js               # writes docs\presentation\FunnelStream-Presentation.pptx and .html
```

The screenshots are copied (and trimmed of empty dark space where needed) from `docs/images` into `docs/presentation/assets`; the originals are not touched. The HTML file needs the `assets` folder beside it.

HTML keys: Right arrow or Space next, Left arrow back, O overview, N speaker notes, T start or reset the 20-minute timer, F full screen, H help.
