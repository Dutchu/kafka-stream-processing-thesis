import { defineConfig } from 'vite';

export default defineConfig({
    server: {
        port: 3000,
        proxy: {
            '/api': 'http://localhost:8080',
            '/ws': { target: 'ws://localhost:8080', ws: true }
        }
    },
    build: {
        outDir: 'dist',
        emptyOutDir: true
    }
});
