run:
	python3 -m venv .venv
	.venv/bin/python3 -m pip install -r requirements.txt
	PYTHONPATH=$(pwd) .venv/bin/python3 -m reddit_corpora_process RC_2014-12.bz2 RC_2014-12.parquet
	PYTHONPATH=$(pwd) .venv/bin/python3 -m reddit_corpora_process RC_2014-12.bz2 RC_2014-12.italian.parquet -subreddits italy,cazzeggio,ITAGLIA,Umorismo,italyLGBT,erba,meditazione,Cibo,ItalyGastronomia,libri,storia,fumetti,tronodispade,ItaliansGoneWild,pieroangela,ploiticaITA,programmazione,italyinformatica,Informatici,Italia,milano,roma,torino,lombaria,lazio,valle_daosta,veneto
