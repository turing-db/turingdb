import axios from 'axios'
import { ThemeProvider, createTheme } from '@mui/material/styles';
import CssBaseline from '@mui/material/CssBaseline';
import { useEffect, useState } from 'react'
import DBSelector from './DBSelector'
import MainFrame from './MainFrame'

function ListAvailableDatabases() {
    const [data, setData] = useState([])
    useEffect(() => {
        axios.get('/api/list_available_databases'
        ).then(res => {
            setData(res.data.db_names);
        }).catch(err => {
            setData(err);
            console.log(err);
        })
    }, [])
    return data;
}

function GetStatus() {
    const [running, setRunning] = useState(false)
    useEffect(() => {
        axios.get('/api/get_status'
        ).then(() => {
            setRunning(true);
        }).catch(() => {
            setRunning(false);
        })
    }, [])
    return running;
}


function App() {
    const darkTheme = createTheme({
        palette: {
            mode: 'dark',
            palette: {
                primary: {
                    dark: '#0d47a1',
                },
                secondary: {
                    dark: '#03a9f4',
                }
            }
        },
    });

    let availableDatabases = ListAvailableDatabases();
    const [selectedDbName, setSelectedDbName] = useState(null);

    if (!GetStatus()) {
        return (
            <ThemeProvider theme={darkTheme}>
                <main>
                    <CssBaseline />
                    <div className="flex justify-center items-center h-screen w-screen
                                    bg-turing1 text-3xl text-secondary">
                        Server is not running
                    </div>
                </main>
            </ThemeProvider>
        );
    }

    return (
        <ThemeProvider theme={darkTheme}>
            <main>
                <CssBaseline />
                <DBSelector
                    available={availableDatabases}
                    setSelectedDbName={setSelectedDbName} />
                <MainFrame db_name={selectedDbName} />
            </main>
        </ThemeProvider>
    );
}

export default App;
