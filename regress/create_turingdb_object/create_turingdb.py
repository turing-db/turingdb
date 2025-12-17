from pytester import TuringdbTester

pytester = TuringdbTester()
pytester.spawn()
t = pytester.client()

print('* create_turingdb_object: done')
