import React from 'react';
import PropTypes from 'prop-types';
import { Bar } from 'react-chartjs-2';

const BarChart = ({ data, title, xAxisLabel, yAxisLabel, legendPosition, responsive, maintainAspectRatio, className }) => {
  const chartData = {
    labels: data.labels,
    datasets: [
      {
        label: data.datasets[0].label,
        data: data.datasets[0].data,
        backgroundColor: data.datasets[0].backgroundColor || 'rgba(54, 162, 235, 0.2)',
        borderColor: data.datasets[0].borderColor || 'rgba(54, 162, 235, 1)',
        borderWidth: data.datasets[0].borderWidth || 1
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
          },
          ticks: {
            beginAtZero: true
          }
        }
      ]
    },
    responsive: responsive,
    maintainAspectRatio: maintainAspectRatio
  };

  return <Bar data={chartData} options={options} className={className} />;
};

BarChart.propTypes = {
  data: PropTypes.shape({
    labels: PropTypes.array.isRequired,
    datasets: PropTypes.arrayOf(
      PropTypes.shape({
        label: PropTypes.string.isRequired,
        data: PropTypes.array.isRequired,
        backgroundColor: PropTypes.string,
        borderColor: PropTypes.string,
        borderWidth: PropTypes.number
      })
    ).isRequired
  }).isRequired,
  title: PropTypes.string,
  xAxisLabel: PropTypes.string,
  yAxisLabel: PropTypes.string,
  legendPosition: PropTypes.oneOf(['top', 'bottom', 'left', 'right']),
  responsive: PropTypes.bool,
  maintainAspectRatio: PropTypes.bool,
  className: PropTypes.string
};

BarChart.defaultProps = {
  title: '',
  xAxisLabel: '',
  yAxisLabel: '',
  legendPosition: 'top',
  responsive: true,
  maintainAspectRatio: true,
  className: ''
};

export default BarChart;
