import turingdb

t = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')

# Create a change
t.set_graph('default')
change = t.query('CHANGE NEW')['Change ID'][0]

# Submit change
t.checkout(change=str(change))
print('Submit change '+str(change))
t.query("CHANGE SUBMIT")

print('* submit_empty_change: done')
