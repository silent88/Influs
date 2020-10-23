from flask import Flask, request, render_template
import pandas as pd
import sqlite3

app = Flask(__name__)

@app.route('/influs')
def show_influs():	
	v_k  = request.args.get("k")
	v_id = request.args.get("igid")
	if (v_id):	
		v_k = v_id
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k or v_id):		
		query = "SELECT * FROM influs WHERE igid='" + v_k + "';"
	else:
		query = "SELECT * FROM influs LIMIT 100;"
	
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='influs')
	
	return render_template('index.html', title="Influs", tables=html_table)
	
@app.route('/influs_propagations_count')
def show_influs_propagations_count():
	v_k  = request.args.get("k")
	v_id = request.args.get("igid")
	if (v_id):	
		v_k = v_id		
	v_rank = request.args.get("rank")
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k and v_rank):		
		query = "SELECT * FROM influs_propagations_count WHERE rank='" + v_rank + "' AND igid='" + v_k + "' ORDER BY c DESC;"
	elif (v_k):
		query = "SELECT * FROM influs_propagations_count WHERE igid='" + v_k + "' ORDER BY c DESC;"
	elif (v_rank):
		query = "SELECT * FROM influs_propagations_count WHERE rank='" + v_rank + "' ORDER BY c DESC;"
	else:
		query = "SELECT * FROM influs_propagations_count ORDER BY c DESC LIMIT 100;"
		
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='influs rank')
	
	return render_template('index.html', title="Influs Propagations Count", tables=html_table)
	
@app.route('/influs_stories_count')
def show_influs_stories_count():
	v_k  = request.args.get("k")
	v_id = request.args.get("igid")
	if (v_id):	
		v_k = v_id		
	v_rank = request.args.get("rank")
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k and v_rank):		
		query = "SELECT * FROM influs_stories_count WHERE rank='" + v_rank + "' AND igid='" + v_k + "' ORDER BY c DESC;"
	elif (v_k):
		query = "SELECT * FROM influs_stories_count WHERE igid='" + v_k + "' ORDER BY c DESC;"
	elif (v_rank):
		query = "SELECT * FROM influs_stories_count WHERE rank='" + v_rank + "' ORDER BY c DESC;"
	else:
		query = "SELECT * FROM influs_stories_count ORDER BY c DESC LIMIT 100;"
		
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='influs rank')
	
	return render_template('index.html', title="Influs Stories Count", tables=html_table)

@app.route('/contracts')
def show_contracts():
	v_k_1  = request.args.get("k1")
	v_id_1 = request.args.get("igid")
	if (v_id_1):	
		v_k_1 = v_id_1
		
	v_k_2  = request.args.get("k2")
	v_id_2 = request.args.get("webproperty_id")
	if (v_id_2):	
		v_k_2 = v_id_2
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k_1 and v_k_2):		
		query = "SELECT * FROM contracts WHERE igid='" + v_k_1 + "' AND webproperty_id='" + v_k_2 + "';"
	elif (v_k_1):
		query = "SELECT * FROM contracts WHERE igid='" + v_k_1 + "';"
	elif (v_k_2):
		query = "SELECT * FROM contracts WHERE webproperty_id='" + v_k_2 + "';"
	else:
		query = "SELECT * FROM contracts LIMIT 100;"
	
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='contracts')
	
	return render_template('index.html', title="Contracts", tables=html_table)

@app.route('/webproperties')
def show_webproperties():
	v_k  = request.args.get("k")
	v_id = request.args.get("webproperty_id")
	if (v_id):	
		v_k = v_id
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k or v_id):		
		query = "SELECT * FROM webproperties WHERE webproperty_id='" + v_k + "';"
	else:
		query = "SELECT * FROM webproperties LIMIT 100;"
		
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='webproperties')
	
	return render_template('index.html', title="Webproperties", tables=html_table)
	
@app.route('/webproperties/json')
def show_webproperties_json():
	v_k  = request.args.get("k")
	v_id = request.args.get("webproperty_id")
	if (v_id):	
		v_k = v_id
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k or v_id):		
		query = "SELECT * FROM webproperties WHERE webproperty_id='" + v_k + "';"
	else:
		query = "SELECT * FROM webproperties LIMIT 100;"
		
	df = pd.read_sql_query(query, conn)
	json_table = df.to_json(orient="index")
	
	return json_table

@app.route('/webproperties_contracts_exposure')
def show_webproperties_contracts_exposure():
	v_k  = request.args.get("k")
	v_id = request.args.get("webproperty_id")
	if (v_id):	
		v_k = v_id		
	v_rank = request.args.get("rank")
	
	conn = sqlite3.connect('../../db/influs2.db')
	if (v_k and v_rank):		
		query = "SELECT * FROM webproperties_contracts_exposure WHERE rank='" + v_rank + "' AND webproperty_id='" + v_k + "';"
	elif (v_k):
		query = "SELECT * FROM webproperties_contracts_exposure WHERE webproperty_id='" + v_k + "';"
	elif (v_rank):
		query = "SELECT * FROM webproperties_contracts_exposure WHERE rank='" + v_rank + "';"
	else:
		query = "SELECT * FROM webproperties_contracts_exposure LIMIT 100;"
		
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='webproperties rank')
	
	return render_template('index.html', title="Webproperties Contracts Exposure", tables=html_table)

@app.route('/propagations')
def show_propagations():
	v_k_1  = request.args.get("k1")
	v_id_1 = request.args.get("igid")
	if (v_id_1):	
		v_k_1 = v_id_1
		
	v_k_2  = request.args.get("k2")
	v_id_2 = request.args.get("webproperty_id")
	if (v_id_2):	
		v_k_2 = v_id_2
	
	conn = sqlite3.connect('../../db/propagations.db')
	if (v_k_1 and v_k_2):		
		query = "SELECT * FROM propagations WHERE igid='" + v_k_1 + "' AND webproperty_id='" + v_k_2 + "';"
	elif (v_k_1):
		query = "SELECT * FROM propagations WHERE igid='" + v_k_1 + "';"
	elif (v_k_2):
		query = "SELECT * FROM propagations WHERE webproperty_id='" + v_k_2 + "';"
	else:
		query = "SELECT * FROM propagations LIMIT 100;"
	
	df = pd.read_sql_query(query, conn)
	html_table = df.to_html(classes='propagations')
	
	return render_template('index.html', title="Propagations", tables=html_table)	
	
if __name__ == "__main__":
    app.run()
