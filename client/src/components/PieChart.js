import React from 'react';
import { Pie } from 'react-chartjs-2';

function PieChart(props) {
  const { data, title, legendPosition, responsive, maintainAspectRatio } = props;

  const options = {
    title: {
      display: true,
      text: title
    },
    legend: {
      display: true,
      position: legendPosition || 'bottom'
    },
    responsive: responsive || true,
    maintainAspectRatio: maintainAspectRatio || false
  };

  return (
    <div>
      <Pie data={data} options={options} />
    </div>
  );
}

export default PieChart;
