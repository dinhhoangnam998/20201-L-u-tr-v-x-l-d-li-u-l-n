import { Box, Container } from "@material-ui/core";
import Page from "./Page";

export default function View({ children, title }) {
  return (
    <div>
      <Page title={title}>
        <Container>
          <Box py={3}>{children}</Box>
        </Container>
      </Page>
    </div>
  );
}
