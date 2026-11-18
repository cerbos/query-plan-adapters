import education from "./education";
import fintech from "./fintech";
import government from "./government";
import healthcare from "./healthcare";
import hr from "./hr";
import iot from "./iot";
import legal from "./legal";
import logistics from "./logistics";
import media from "./media";
import retail from "./retail";

export const MAPPINGS = {
  healthcare,
  fintech,
  logistics,
  education,
  legal,
  retail,
  hr,
  media,
  iot,
  government,
};

export type DomainName = keyof typeof MAPPINGS;
