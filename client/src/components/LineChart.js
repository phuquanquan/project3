import React from 'react';
import PropTypes from 'prop-types';
import { Line } from 'react-chartjs-2';

const LineChart = ({ data, title, xAxisLabel, yAxisLabel, legendPosition, responsive, maintainAspectRatio, className }) => {
  const chartData = {
    labels: data.map(item => item.date),
    datasets: [
      {
        label: 'Total Requests',
        data: data.map(item => item.totalRequests),
        borderColor: 'rgba(75,192,192,1)',
        borderWidth: 2
      },
      {
        label: 'Total Errors',
        data: data.map(item => item.totalErrors),
        borderColor: 'rgba(255,99,132,1)',
        borderWidth: 2
      }
    ]
  };

  const options = {
    title: {
      display: true,
      text: title
    },
    legend: {
      display: true,
      position: legendPosition
    },
    scales: {
      xAxes: [
        {
          scaleLabel: {
            display: true,
            labelString: xAxisLabel
          }
        }
      ],
      yAxes: [
        {
          scaleLabel: {
            display: true,
            labelString: yAxisLabel
          }
        }
      ]
    },
    responsive: responsive,
    maintainAspectRatio: maintainAspectRatio
  };

  return <Line data={chartData} options={options} className={className} />;
};

LineChart.propTypes = {
  data: PropTypes.array.isRequired,
  title: PropTypes.string,
  xAxisLabel: PropTypes.string,
  yAxisLabel: PropTypes.string,
  legendPosition: PropTypes.oneOf(['top', 'bottom', 'left', 'right']),
  responsive: PropTypes.bool,
  maintainAspectRatio: PropTypes.bool,
  className: PropTypes.string
};

LineChart.defaultProps = {
  title: '',
  xAxisLabel: '',
  yAxisLabel: '',
  legendPosition: 'top',
  responsive: true,
  maintainAspectRatio: true,
  className: ''
};

export default LineChart;
