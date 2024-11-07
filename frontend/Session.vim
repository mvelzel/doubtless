let SessionLoad = 1
let s:so_save = &g:so | let s:siso_save = &g:siso | setg so=0 siso=0 | setl so=-1 siso=-1
let v:this_session=expand("<sfile>:p")
silent only
silent tabonly
cd ~/doubtless/frontend
if expand('%') == '' && !&modified && line('$') <= 1 && getline(1) == ''
  let s:wipebuf = bufnr('%')
endif
let s:shortmess_save = &shortmess
if &shortmess =~ 'A'
  set shortmess=aoOA
else
  set shortmess=aoO
endif
badd +3 ~/doubtless/frontend/src/main.tsx
badd +31 ~/doubtless/frontend/node_modules/.pnpm/@tanstack+react-router@1.77.8_@tanstack+router-generator@1.77.7_react-dom@18.3.1_react@18.3.1__react@18.3.1/node_modules/@tanstack/react-router/dist/esm/RouterProvider.d.ts
badd +1 src/routeTree.gen.ts
badd +442 ~/doubtless/frontend/node_modules/.pnpm/@tanstack+react-router@1.77.8_@tanstack+router-generator@1.77.7_react-dom@18.3.1_react@18.3.1__react@18.3.1/node_modules/@tanstack/react-router/dist/esm/router.d.ts
badd +1 ~/doubtless/frontend/src/routes/index.tsx
badd +2 ~/doubtless/frontend/src/routes/about.tsx
badd +36 ~/doubtless/frontend/src/routes/__root.tsx
badd +0 term://~/doubtless/frontend//22266:/bin/zsh
badd +10 term://~/doubtless/frontend//22384:/bin/zsh
badd +0 term://~/doubtless/frontend//22690:/bin/zsh
argglobal
%argdel
tabnew +setlocal\ bufhidden=wipe
tabrewind
edit ~/doubtless/frontend/src/main.tsx
let s:save_splitbelow = &splitbelow
let s:save_splitright = &splitright
set splitbelow splitright
wincmd _ | wincmd |
vsplit
1wincmd h
wincmd w
let &splitbelow = s:save_splitbelow
let &splitright = s:save_splitright
wincmd t
let s:save_winminheight = &winminheight
let s:save_winminwidth = &winminwidth
set winminheight=0
set winheight=1
set winminwidth=0
set winwidth=1
exe 'vert 1resize ' . ((&columns * 157 + 158) / 316)
exe 'vert 2resize ' . ((&columns * 158 + 158) / 316)
argglobal
balt ~/doubtless/frontend/node_modules/.pnpm/@tanstack+react-router@1.77.8_@tanstack+router-generator@1.77.7_react-dom@18.3.1_react@18.3.1__react@18.3.1/node_modules/@tanstack/react-router/dist/esm/router.d.ts
setlocal fdm=manual
setlocal fde=0
setlocal fmr={{{,}}}
setlocal fdi=#
setlocal fdl=0
setlocal fml=1
setlocal fdn=20
setlocal fen
silent! normal! zE
let &fdl = &fdl
let s:l = 3 - ((2 * winheight(0) + 37) / 75)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 3
normal! 026|
wincmd w
argglobal
if bufexists(fnamemodify("~/doubtless/frontend/src/routes/__root.tsx", ":p")) | buffer ~/doubtless/frontend/src/routes/__root.tsx | else | edit ~/doubtless/frontend/src/routes/__root.tsx | endif
if &buftype ==# 'terminal'
  silent file ~/doubtless/frontend/src/routes/__root.tsx
endif
balt ~/doubtless/frontend/src/routes/about.tsx
setlocal fdm=manual
setlocal fde=0
setlocal fmr={{{,}}}
setlocal fdi=#
setlocal fdl=0
setlocal fml=1
setlocal fdn=20
setlocal fen
silent! normal! zE
let &fdl = &fdl
let s:l = 36 - ((35 * winheight(0) + 37) / 75)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 36
normal! 0
wincmd w
exe 'vert 1resize ' . ((&columns * 157 + 158) / 316)
exe 'vert 2resize ' . ((&columns * 158 + 158) / 316)
tabnext
let s:save_splitbelow = &splitbelow
let s:save_splitright = &splitright
set splitbelow splitright
wincmd _ | wincmd |
vsplit
1wincmd h
wincmd w
let &splitbelow = s:save_splitbelow
let &splitright = s:save_splitright
wincmd t
let s:save_winminheight = &winminheight
let s:save_winminwidth = &winminwidth
set winminheight=0
set winheight=1
set winminwidth=0
set winwidth=1
exe 'vert 1resize ' . ((&columns * 157 + 158) / 316)
exe 'vert 2resize ' . ((&columns * 158 + 158) / 316)
argglobal
if bufexists(fnamemodify("term://~/doubtless/frontend//22384:/bin/zsh", ":p")) | buffer term://~/doubtless/frontend//22384:/bin/zsh | else | edit term://~/doubtless/frontend//22384:/bin/zsh | endif
if &buftype ==# 'terminal'
  silent file term://~/doubtless/frontend//22384:/bin/zsh
endif
balt ~/doubtless/frontend/src/routes/__root.tsx
setlocal fdm=manual
setlocal fde=0
setlocal fmr={{{,}}}
setlocal fdi=#
setlocal fdl=0
setlocal fml=1
setlocal fdn=20
setlocal fen
let s:l = 10 - ((9 * winheight(0) + 37) / 75)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 10
normal! 025|
wincmd w
argglobal
if bufexists(fnamemodify("term://~/doubtless/frontend//22690:/bin/zsh", ":p")) | buffer term://~/doubtless/frontend//22690:/bin/zsh | else | edit term://~/doubtless/frontend//22690:/bin/zsh | endif
if &buftype ==# 'terminal'
  silent file term://~/doubtless/frontend//22690:/bin/zsh
endif
balt term://~/doubtless/frontend//22384:/bin/zsh
setlocal fdm=manual
setlocal fde=0
setlocal fmr={{{,}}}
setlocal fdi=#
setlocal fdl=0
setlocal fml=1
setlocal fdn=20
setlocal fen
let s:l = 17 - ((16 * winheight(0) + 37) / 75)
if s:l < 1 | let s:l = 1 | endif
keepjumps exe s:l
normal! zt
keepjumps 17
normal! 0
wincmd w
exe 'vert 1resize ' . ((&columns * 157 + 158) / 316)
exe 'vert 2resize ' . ((&columns * 158 + 158) / 316)
tabnext 1
if exists('s:wipebuf') && len(win_findbuf(s:wipebuf)) == 0 && getbufvar(s:wipebuf, '&buftype') isnot# 'terminal'
  silent exe 'bwipe ' . s:wipebuf
endif
unlet! s:wipebuf
set winheight=1 winwidth=20
let &shortmess = s:shortmess_save
let &winminheight = s:save_winminheight
let &winminwidth = s:save_winminwidth
let s:sx = expand("<sfile>:p:r")."x.vim"
if filereadable(s:sx)
  exe "source " . fnameescape(s:sx)
endif
let &g:so = s:so_save | let &g:siso = s:siso_save
set hlsearch
let g:this_session = v:this_session
let g:this_obsession = v:this_session
doautoall SessionLoadPost
unlet SessionLoad
" vim: set ft=vim :
