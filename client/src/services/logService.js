import axios from 'axios';

const API_URL = 'http://example.com/api';

// Lấy dữ liệu logs từ server
const getLogs = async () => {
  const response = await axios.get(`${API_URL}/logs`);
  return response.data;
};

export default {
  getLogs
};
