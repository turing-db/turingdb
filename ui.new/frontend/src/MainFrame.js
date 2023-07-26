import Button from '@mui/material/Button';
import { useEffect, useState } from 'react'
import axios from 'axios'

const DBInfos = (props) => {
    return (
        <div>
            Database: {props.db.db_name} (Id: {props.db.db_id})
        </div>
    );
};

const DBFrame = (props) => {
    const [db, setDb] = useState({});
    const [isDbLoaded, setIsDbLoaded] = useState(false);

    useEffect(() => {
        axios.get('/api/is_db_loaded', { params: { db_name: props.db_name } })
            .then(res => {
                setIsDbLoaded(res.data.loaded);
            }).catch(err => {
                console.log(err);
            })
    }, [props.db_name]);

    useEffect(() => {
        console.log(props.db_name, ":", isDbLoaded);
        if (isDbLoaded) {
            console.log("Getting db:", props.db_name, ". Loaded:", isDbLoaded);
            //axios.post('/api/get_database', { db_name: props.db_name })
            //    .then(res => {
            //        console.log("In is_db_loaded:", res.data);
            //        setDb(res.data);
            //    }).catch(err => {
            //        console.log(err);
            //        setDb({})
            //    });
        }
    }, [isDbLoaded, props.db_name]);

    const onClick = () => {
        axios.post('/api/load_database', { db_name: props.db_name }).then(res => {
            console.log("In onclick:", res.data);
            setDb(res.data)
            setIsDbLoaded(true);
        }).catch(err => {
            console.log(err);
            setDb({});
        })
    }

    return (
        <div className="flex-col text-center ">
            {
                isDbLoaded
                    ? <DBInfos db={db} />
                    : <div><div>Database is not loaded</div>
                        <div className="p-10">
                            <Button
                                variant="contained"
                                color="primary"
                                onClick={onClick}>
                                Load database
                            </Button>
                        </div></div>
            }
        </div>
    )
};

const MainFrame = (props) => {
    return (
        <div className="flex justify-center items-center h-screen w-screen bg-turing1">
            {props.db_name
                ? <DBFrame db_name={props.db_name} />
                : <img src="/turing_illust.png"
                    className="w-1/3"
                    alt="Turing Biosystems" />}
        </div>
    );
};

export default MainFrame;
