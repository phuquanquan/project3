import React, { useEffect } from 'react';
import { useDispatch, useSelector } from 'react-redux';
import logService from '../services/logService';
import LineChart from './LineChart';
import BarChart from './BarChart';
import PieChart from './PieChart';

const Dashboard = () => {
  const dispatch = useDispatch();
  const logs = useSelector(state => state.logs.logs);

  useEffect(() => {
    dispatch({ type: 'FETCH_LOGS_REQUEST' });
    logService.getLogs()
      .then(data => {
        dispatch({
          type: 'FETCH_LOGS_SUCCESS',
          payload: data
        });
      })
      .catch(error => {
        dispatch({
          type: 'FETCH_LOGS_FAILURE',
          payload: error.message
        });
      });
  }, [dispatch]);

  return (
    <div>
      <h2>Dashboard</h2>
      {logs.length > 0 &&
        <>
          <h3>Total Requests and Errors</h3>
          <LineChart logs={logs} />
          <h3>Requests by Status Code</h3>
          <BarChart logs={logs} />
          <h3>Requests by Method</h3>
          <PieChart data={logs} />
        </>
      }
    </div>
  );
};

export default Dashboard;
