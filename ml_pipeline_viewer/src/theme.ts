import { createGlobalStyle } from 'styled-components'


export const GlobalStyle = createGlobalStyle`
  html, body, #root, .radix-themes {
    height: 100%;
  }

  body {
    padding: 0;
    margin: 0;
    min-width: 1200px;
  }
  
`