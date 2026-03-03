module.exports = {
  apps: [
    {
      name: 'crabify',
      script: 'python3',
      args: '/home/user/webapp/server.py',
      cwd: '/home/user/webapp',
      watch: false,
      instances: 1,
      exec_mode: 'fork',
      env: {
        PORT: 3000
      }
    }
  ]
}
