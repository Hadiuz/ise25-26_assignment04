package de.seuhd.campuscoffee.domain.impl;

import de.seuhd.campuscoffee.domain.exceptions.DuplicatePosNameException;
import de.seuhd.campuscoffee.domain.exceptions.OsmNodeMissingFieldsException;
import de.seuhd.campuscoffee.domain.exceptions.OsmNodeNotFoundException;
import de.seuhd.campuscoffee.domain.model.CampusType;
import de.seuhd.campuscoffee.domain.model.Pos;
import de.seuhd.campuscoffee.domain.exceptions.PosNotFoundException;
import de.seuhd.campuscoffee.domain.model.PosType;
import de.seuhd.campuscoffee.domain.ports.OsmDataService;
import de.seuhd.campuscoffee.domain.ports.PosDataService;
import de.seuhd.campuscoffee.domain.ports.PosService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.NonNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Objects;

/**
 * Implementation of the POS service that handles business logic related to POS entities.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PosServiceImpl implements PosService {
    private final PosDataService posDataService;
    private final OsmDataService osmDataService;

    @Override
    public void clear() {
        log.warn("Clearing all POS data");
        posDataService.clear();
    }

    @Override
    public @NonNull List<Pos> getAll() {
        log.debug("Retrieving all POS");
        return posDataService.getAll();
    }

    @Override
    public @NonNull Pos getById(@NonNull Long id) throws PosNotFoundException {
        log.debug("Retrieving POS with ID: {}", id);
        return posDataService.getById(id);
    }

    @Override
    public @NonNull Pos upsert(@NonNull Pos pos) throws PosNotFoundException {
        if (pos.id() == null) {
            // Create new POS
            log.info("Creating new POS: {}", pos.name());
            return performUpsert(pos);
        } else {
            // Update existing POS
            log.info("Updating POS with ID: {}", pos.id());
            // POS ID must be set
            Objects.requireNonNull(pos.id());
            // POS must exist in the database before the update
            posDataService.getById(pos.id());
            return performUpsert(pos);
        }
    }

    @Override
    public @NonNull Pos importFromOsmNode(@NonNull Long nodeId) throws OsmNodeNotFoundException {
        log.info("Importing POS from OpenStreetMap node {}...", nodeId);
        RestTemplate restTemplate = new RestTemplate();
        String url = "https://api.openstreetmap.org/api/0.6/node/" + nodeId + ".json";
        String jsonResponse;
        try {
            jsonResponse = restTemplate.getForObject(url, String.class);
        } catch (Exception e) {
            throw new OsmNodeNotFoundException(nodeId);
        }

        if (jsonResponse == null) {
            throw new OsmNodeNotFoundException(nodeId);
        }

        try {
            JSONObject responseJson = new JSONObject(jsonResponse);
            JSONArray elements = responseJson.getJSONArray("elements");

            if (elements.length() == 0) {
                throw new OsmNodeNotFoundException(nodeId);
            }

            JSONObject nodeElement = elements.getJSONObject(0);
            JSONObject tags = nodeElement.optJSONObject("tags");

            if (tags == null) {
                throw new OsmNodeMissingFieldsException(nodeId);
            }

            String name = tags.optString("name", null);
            if (name == null) {
                throw new OsmNodeMissingFieldsException(nodeId);
            }

            String description = tags.optString("description", "N/A");
            String street = tags.optString("addr:street", "Unknown");
            String houseNumber = tags.optString("addr:housenumber", "N/A");
            if(houseNumber.equals("N/A"))houseNumber = "0";
            String city = tags.optString("addr:city", "Unknown");
            String postcode = tags.optString("addr:postcode", "0");

            String amenity = tags.optString("amenity", "Unknown");
            String shop = tags.optString("shop", "Unknown");
            String typeString = amenity.equals("Unknown") ? shop : amenity;

            PosType type = PosType.UNKNOWN;
            switch (typeString){
                case "cafe":
                    type = PosType.CAFE;
                    break;
                case "vending_machine":
                    type = PosType.VENDING_MACHINE;
                    break;
                case "bakery":
                    type = PosType.BAKERY;
                    break;
            }

            CampusType campusType = CampusType.UNKNOWN;
            switch (postcode){
                case "69117":
                    campusType = CampusType.ALTSTADT;
                    break;
                case "69115":
                    campusType = CampusType.BERGHEIM;
                    break;
                case  "69120":
                    campusType = CampusType.INF;
                    break;
            }

            Pos.PosBuilder posBuilder = Pos.builder()
                    .name(name)
                    .description(description)
                    .type(type)
                    .campus(campusType)
                    .street(street)
                    .houseNumber(houseNumber)
                    .city(city);

            try {
                posBuilder.postalCode(Integer.parseInt(postcode));
            } catch (NumberFormatException e) {
                log.warn("Could not parse postcode '{}' for OSM node {}. Using default 0.", postcode, nodeId);
                posBuilder.postalCode(0);
            }

            Pos pos = posBuilder.build();
            Pos savedPos = this.upsert(pos);
            log.info("Successfully imported POS '{}' from OSM node {}", savedPos.name(), nodeId);
            return savedPos;

        } catch (Exception e) {
            log.error("Failed to parse OSM node data for node ID: {}", nodeId, e);
            throw new OsmNodeMissingFieldsException(nodeId);
        }
    }

    /**
     * Performs the actual upsert operation with consistent error handling and logging.
     * Database constraint enforces name uniqueness - data layer will throw DuplicatePosNameException if violated.
     * JPA lifecycle callbacks (@PrePersist/@PreUpdate) set timestamps automatically.
     *
     * @param pos the POS to upsert
     * @return the persisted POS with updated ID and timestamps
     * @throws DuplicatePosNameException if a POS with the same name already exists
     */
    private @NonNull Pos performUpsert(@NonNull Pos pos) throws DuplicatePosNameException {
        try {
            Pos upsertedPos = posDataService.upsert(pos);
            log.info("Successfully upserted POS with ID: {}", upsertedPos.id());
            return upsertedPos;
        } catch (DuplicatePosNameException e) {
            log.error("Error upserting POS '{}': {}", pos.name(), e.getMessage());
            throw e;
        }
    }

}
