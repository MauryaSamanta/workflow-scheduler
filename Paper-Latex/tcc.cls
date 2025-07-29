\NeedsTeXFormat{LaTeX2e}
\ProvidesClass{tcc}[2024/02/01 TCC]

\LoadClass[12pt,a4paper]{report}

%% Load additional packages and commands.
\RequirePackage[a4paper, left=2.5cm, right=2.5cm, top=2.5cm, bottom=2.5cm]{geometry} 
\RequirePackage[utf8]{inputenc} 
\RequirePackage[square,authoryear]{natbib}
\RequirePackage[dvipsnames,svgnames,table,xcdraw]{xcolor}
\RequirePackage{amsmath}
\RequirePackage{booktabs}
\RequirePackage{colortbl}
\RequirePackage[acronym,nomain]{glossaries}
\RequirePackage{fancyhdr} 
\setlength{\headheight}{14.5pt}
\RequirePackage{float} 
\RequirePackage{framed}
\RequirePackage{graphicx}
\RequirePackage[hidelinks]{hyperref}
\RequirePackage{indentfirst}
\RequirePackage{multicol}
\RequirePackage{multirow}
\RequirePackage{ragged2e}
\RequirePackage{xcolor}
\RequirePackage{sectsty}
\RequirePackage{setspace}
\RequirePackage{titlesec}
\RequirePackage{tocloft}
\RequirePackage{wrapfig} 
\RequirePackage{mathptmx}
\RequirePackage{subfig}	
\RequirePackage[labelfont=bf]{caption}
\RequirePackage{pdfpages}
\RequirePackage{chngcntr}
\counterwithout{figure}{chapter}
\counterwithout{table}{chapter}
\RequirePackage{amssymb}
\RequirePackage[noend]{algpseudocode}
\RequirePackage{comment}


%acronym
\newcommand{\acindex}[1]{\gls{#1}\index{#1}}
\newcommand{\aclong}[1]{\acrlong{#1}\index{#1}}
\newcommand{\acshort}[1]{\acrshort{#1}\index{#1}}
\newcommand{\acfull}[1]{\acrfull{#1}\index{#1}}

%% Additional TeX/LaTeX code...
\makeatletter
\def\BState{\State\hskip-\ALG@thistlm}
\makeatother

\renewcommand {\baselinestretch}{1.5}
\setlength\parindent{2.25cm} 

\pagestyle{fancy}
\fancyhf{}
\fancyheadoffset{0cm}
\renewcommand{\headrulewidth}{0pt} 
\renewcommand{\footrulewidth}{0pt}
\fancyhf{}                     % clear all header/footer fields
\fancyfoot[C]{\thepage}        % page number in center of footer
\renewcommand{\headrulewidth}{0pt}
\renewcommand{\footrulewidth}{0pt}

\fancypagestyle{plain}{
	 \fancyhf{}
    \fancyfoot[C]{\thepage}  % ✅ bottom-center for plain pages (like chapter starts)
    \renewcommand{\headrulewidth}{0pt}
    \renewcommand{\footrulewidth}{0pt}
}

\chapterfont{}
\sectionfont{\normalsize \bfseries \flushleft}



\titleformat{\section}[hang] 
{\fontsize{14pt}{\baselineskip}\selectfont\bfseries}{\thesection}{2em}{}
\titlespacing*{\section}{0pt}{1em}{1em}

\titleformat{\subsection}[hang] 
{\fontsize{14pt}{\baselineskip}\selectfont\bfseries}{\thesubsection}{2em}{}
\titlespacing*{\subsection}{0pt}{1em}{1em}

\setlength{\cftbeforetoctitleskip}{-2em}
\setlength{\cftaftertoctitleskip}{2em}
\renewcommand{\cfttoctitlefont}{\hfill\large\bfseries} 
\renewcommand{\cftaftertoctitle}{\hfill}

\pagestyle{fancy}

\fancyhf{}                            % clear all headers and footers
\fancyfoot[C]{\thepage}              % ✅ page number in bottom-center
\renewcommand{\headrulewidth}{0pt}
\renewcommand{\footrulewidth}{0pt}

\fancypagestyle{plain}{
	 \fancyhf{}
    \fancyfoot[C]{\thepage}  % ✅ bottom-center for plain pages (like chapter starts)
    \renewcommand{\headrulewidth}{0pt}
    \renewcommand{\footrulewidth}{0pt}
}

\chapterfont{}
\sectionfont{\normalsize \bfseries \flushleft}


\titleformat{\chapter}[display] 
{\fontsize{18pt}{\baselineskip}\selectfont\bfseries}{}{0em}{}
\titlespacing*{\chapter}{0pt}{-4em}{0em}

\titleformat{\section}[hang] 
{\fontsize{14pt}{\baselineskip}\selectfont\bfseries}{\thesection}{2em}{}
\titlespacing*{\section}{0pt}{1em}{1em}

\titleformat{\subsection}[hang] 
{\fontsize{14pt}{\baselineskip}\selectfont\bfseries}{\thesubsection}{2em}{}
\titlespacing*{\subsection}{0pt}{1em}{1em}

\setlength{\cftbeforetoctitleskip}{-2em}
\setlength{\cftaftertoctitleskip}{2em}
\renewcommand{\cfttoctitlefont}{\hfill\large\bfseries} 
\renewcommand{\cftaftertoctitle}{\hfill}



% Centraliza Lista de Tabelas
\setlength{\cftbeforelottitleskip}{-2em}
\setlength{\cftafterlottitleskip}{2em}
\renewcommand{\cftlottitlefont}{\hfill\bfseries\large} 
\renewcommand{\cftafterlottitle}{\hfill}
\newlength{\mylen}
\renewcommand{\cfttabpresnum}{TABLE\enspace}
\renewcommand{\cfttabaftersnum}{:}
\settowidth{\mylen}{\cfttabpresnum\cfttabaftersnum}
\addtolength{\cfttabnumwidth}{\mylen}

% Centraliza Lista de Figuras
\setlength{\cftbeforeloftitleskip}{-2em}
\setlength{\cftafterloftitleskip}{2em}
\renewcommand{\cftloftitlefont}{\hfill\bfseries\large} 
\renewcommand{\cftafterloftitle}{\hfill}
\renewcommand{\cftfigpresnum}{FIGURE\enspace}
\renewcommand{\cftfigaftersnum}{:}
\settowidth{\mylen}{\cftfigpresnum\cftfigaftersnum}
\addtolength{\cftfignumwidth}{\mylen}

\newcolumntype{L}[1]{>{\raggedright\let\newline\\\arraybackslash\hspace{0pt}}m{#1}}
\newcolumntype{C}[1]{>{\centering\let\newline\\\arraybackslash\hspace{0pt}}m{#1}}
\newcolumntype{R}[1]{>{\raggedleft\let\newline\\\arraybackslash\hspace{0pt}}m{#1}}

\newcommand\Tstrut{\rule{0pt}{2.6ex}} % = `top' strut
\newcommand\Bstrut{\rule[-0.9ex]{0pt}{0pt}} % = `bottom' strut
\newcommand{\specialcell}[2][c]{%
	\begin{tabular}[#1]{@{}c@{}}#2\end{tabular}}

%dcover
\newcommand\dcover{%
    \thispagestyle{empty} % No page number
    \newgeometry{left=2.5cm, right=2.5cm, top=2.5cm, bottom=2.5cm}

    \begin{center}
        \vspace*{0.20\textheight} % Push content downward

        {\bfseries \LARGE Deadline and Reliability Constrained Workflow Scheduling via Analytical Replica Estimation}\\[2em]

        {\normalsize\mdseries as part of Research Internship}\\[10em]

        \vfill

        {\normalsize \mdseries
        \begin{flushright}
            \dauthor
        \end{flushright}}

        \vspace{2em}

        \begin{flushright}
            \begin{minipage}{0.5\textwidth}
                \raggedleft
                under the supervision of \\
                \dadvisor
            \end{minipage}
        \end{flushright}
    \end{center}
}

%dcoverback
\newcommand\dcoverback{%
	%\begin{titlepage}
	\thispagestyle{empty} % sem numeracao
	\newgeometry{left=2.5cm, right=2.5cm, top=2.5cm, bottom=2.5cm}
	\center % Center everything on the page
	
	\begin{minipage}{\textwidth}
		\begin{center}
			{\bfseries \large CEFET/RJ - CENTRO FEDERAL DE EDUCAÇÃO TECNOLÓGICA CELSO SUCKOW DA FONSECA}\\[6em]
			{\bfseries \LARGE \dtitle}\\[6em]% Title of your document
			\raggedleft \dauthor
		\end{center}
	\end{minipage}\\[6em]
	
	
	
	\begin{flushright}
		\begin{minipage}{0.5\textwidth}
			\raggedleft
			
			\dadvisor
		\end{minipage}\\
	\end{flushright}
	\vfill	
	
}

%dform

\newcommand{\dlibrary}[1]{
	\includepdf{#1} % página pdf ou png feita pela biblioteca
	\center % Center everything on the page
	\vfill	
	\pagebreak
}

%ddedicatory
\newcommand{\ddedicatory}[1]{
	{\fontsize{16pt}{\baselineskip}\selectfont \textbf{DEDICATÓRIA}}
	%\thispagestyle{empty}
	\vspace*{15em}%
	\begin{flushright}
		\begin{minipage}{0.5\textwidth}
			#1
		\end{minipage}\\[1.5cm]
	\end{flushright}
	\pagebreak
}

%ddedicatory
\newcommand{\dacknowledgment}[1]{
	
{\fontsize{16pt}{\baselineskip}\selectfont \textbf{AGRADECIMENTOS}}
%\thispagestyle{empty}
\vspace*{10em}%
\begin{flushright}
	\begin{minipage}{0.5\textwidth}
		#1
	\end{minipage}\\[1.5cm]
\end{flushright}

\pagebreak
}

%dresumo
\newcommand{\dresumo}[2]{
\begin{center}
	{\fontsize{16pt}{\baselineskip}\selectfont \textbf{RESUMO}}\\[2em]
\end{center}

\justifying
\noindent
#1
\\[3em]

\normalsize\noindent
\textbf{Palavras-chaves}: #2

\pagebreak
}

%dabstract
\newcommand{\dabstract}[2]{
	\begin{center}
		{\fontsize{16pt}{\baselineskip}\selectfont \textbf{ABSTRACT}}\\[2em]
	\end{center}
	
	\justifying
	\noindent
	#1
	\\[3em]
	
	\normalsize\noindent
	\textbf{Keywords}: #2
	
	\pagebreak
}

\renewcommand{\glstextformat}[1]{\textit{#1}}


%dtables
\newcommand{\dtables}{

%\renewcommand{\backrefpagesname}{Citado na(s) página(s):~}
%\renewcommand{\backref}{Citado na(s) página(s):~}

\addto\captionsenglish{\renewcommand*\contentsname{\fontsize{16pt}{\baselineskip}\selectfont\textbf{SUMÁRIO}}}
\addto\captionsenglish{\renewcommand*\listtablename{\fontsize{16pt}{\baselineskip}\selectfont \textbf{LISTA DE TABELAS}}}
\addto\captionsenglish{\renewcommand*\listfigurename{\fontsize{16pt}{\baselineskip}\selectfont \textbf{LISTA DE FIGURAS}}}	
	
\renewcommand{\cftdot}{}
\tableofcontents
\pagebreak

\listoffigures
\pagebreak

\listoftables
\pagebreak

\clearpage


%\printterms[database=acronym,columns=1,prelocation=hfill,style=align]
%\label{lastpretextualpage}
%\pagebreak
}




\endinput