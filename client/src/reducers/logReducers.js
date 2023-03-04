const initialState = {
    logs: [],     // Dữ liệu logs
    isLoading: false,
    error: null
  };
  
  const logsReducer = (state = initialState, action) => {
    switch (action.type) {
      case 'FETCH_LOGS_REQUEST':
        return {
          ...state,
          isLoading: true,
          error: null
        };
      case 'FETCH_LOGS_SUCCESS':
        return {
          ...state,
          logs: action.payload,
          isLoading: false,
          error: null
        };
      case 'FETCH_LOGS_FAILURE':
        return {
          ...state,
          isLoading: false,
          error: action.payload
        };
      default:
        return state;
    }
  };
  
  export default logsReducer;
  