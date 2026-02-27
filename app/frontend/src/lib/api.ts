// Mock client for local development without @metagptx/web-sdk
export const client = {
  auth: {
    login: () => {
      console.log('Mock login called');
    }
  }
};
